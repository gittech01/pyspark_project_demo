import sys
import boto3
import logging
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F, Window
from pyspark.sql.utils import AnalysisException
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)



client = boto3.client('glue')

def list_tables(database_name):
    table_names = []
    try:
        response = client.get_tables(DatabaseName=database_name)
        tables = response['TableList']
        table_names = [table['Name'] for table in tables]
    except Exception as e:
        print(f"Error fetching tables: {e}")
    return table_names


def get_table_schema(database_name, table_name):
    try:
        response = client.get_table(DatabaseName=database_name, Name=table_name)
        table = response['Table']
        schema = table['StorageDescriptor']['Columns']
        
        return schema
    except Exception as e:
        print(f"Error fetching schema for table {table_name}: {e}")
        return []


def get_all_schemas(database_name):
    tables = list_tables(database_name)
    schemas = {}
    for table in tables:
        schemas[table] = get_table_schema(database_name, table)
    else:
        mappings_table = {}
        for table, columns in schemas.items():
            mappings_columns = [(col['Name'], col['Type']) for col in columns]
            mappings_table[table] = mappings_columns
        return mappings_table



def get_envs():
    return getResolvedOptions(sys.argv,[
            'JOB_NAME', 'BUCKET_STAGE'
            , 'BUCKET_SOR' 
            , 'DATABASE_SOR'
        ])


def init_context():
    spark_context = SparkContext()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session
    
    return glue_context, spark


def init_job(glue_context, args):
    job_init = Job(glue_context)
    job_init.init(args['JOB_NAME'], args)
    return job_init


def init_glue_client():
    glue_client = boto3.client('glue')
    return glue_client
    

def get_anomes():
    return datetime.now().strftime('%Y%m')
    

def read_raw_info(spark_session, s3_bucket_stage):
    try:
        frame = (
            spark_session
            .read
            .format("json")
            .option("recursiveFileLookup", "true")
            .option("multiline", "true")
            .load(s3_bucket_stage)
        )
    except Exception as err:
        print(f"Error: {error}")
        
    return frame
    

def select_explode_columns_spark_frame(frame):
    
    select_explode_dataframe = (
            frame
            .select(
                F.col("name").alias("new_name"),
                F.explode(F.col("packages")).alias("pacotes")
            )
            .select("new_name", "pacotes.*")
        )
        
    return select_explode_dataframe, select_explode_dataframe.columns


def create_select_columns_spark_frame(frame, cols: list):
    COLS = ["name", "versionInfo", "licenseConcluded"]
    
    for col in COLS:
        new_name_col = (
                    "new_biblioteca" if col == COLS[0]
            else "versao_biblioteca" if col == COLS[1]
            else "licenca_biblioteca"
        )
        if col not in cols:
            frame = frame.withColumn(new_name_col, F.lit(None))
            continue
        frame = frame.withColumnRenamed(col, new_name_col)

    return frame
    

def transform_frame(sparkSession, bucket_stage, ano_mes):
    frame = read_raw_info(sparkSession, bucket_stage)
    frame, cols = select_explode_columns_spark_frame(frame)
    frame = create_select_columns_spark_frame(frame, cols)

    frame = frame.select(
                    F.col("new_name")
                    , F.col("new_biblioteca")
                    , F.col("versao_biblioteca")
                    , F.col("licenca_biblioteca")
                )
    
    final_frame = frame.filter(frame.new_name != frame.new_biblioteca)
    final_frame = final_frame.withColumn("nome_repositorio", F.split(F.col("new_name"), '/')[1])
    final_frame = final_frame.withColumn("gerenciador_biblioteca", F.split(F.col("new_biblioteca"), ':')[0])
    final_frame = final_frame.withColumn("nome_biblioteca", F.split(F.col("new_biblioteca"), ':')[1])

    final_frame = (
            final_frame
            .select(*[col for col in final_frame.columns if col not in ('new_name','new_biblioteca')])
            .withColumn('anomes', F.lit(ano_mes))
        )
    
    return final_frame
    

def create_micro_frame_with_domain(column_name, frame):
    frame_r = frame.select(F.col(column_name)).distinct()
    return frame_r
    

def view_table_glue(glue_context, database_sor, table_name_sor):
    dynamic_frame = (
        glue_context
        .create_dynamic_frame
        .from_catalog(
            database=database_sor, 
            table_name=table_name_sor
        )
    )
    
    ps_frame = dynamic_frame.toDF()
    return ps_frame
    

def create_hash_validate(frame, columns: list):
    hash_valiadte = (
        frame.withColumn(
            'id_check',
            F.md5(F.concat_ws(':', *columns))
        ))
    return hash_valiadte

    
def frame_insert(left_frame, right_frame, how_:str='left_anti'):
    result = left_frame.join(right_frame, on='id_check', how=how_)
    result = result.select(*[col for col in result.columns if col not in ('id_check',)])
    print('Frame insert:')
    result.printSchema()
    return result
    

def inser_data_db(
    database_sor,
    table_name_sor,
    frame,
    glue_context,
    particao: str = None
):
    
    dynamic_frame = DynamicFrame.fromDF(frame, glue_context, "dynamic_frame")
    glue_context.write_dynamic_frame.from_catalog(
        frame=dynamic_frame,
        database=database_sor,
        table_name=table_name_sor,
        additional_options={
            "partitionKeys": [particao] if particao else []
        },
        transformation_ctx="dataSink"
    )

    

def main():
    
    logger.info('===============   START JOB   ================')
    args = get_envs()
    
    gluecontext, sparkSession = init_context()
    job_init = init_job(gluecontext, args)
    glue_client = init_glue_client()
    
    ano_mes = get_anomes()
    bucket_stage = args['BUCKET_STAGE']
    bucket_sor = args['BUCKET_SOR']
    database_sor = args['DATABASE_SOR']
    
    ps_frame = transform_frame(sparkSession, bucket_stage, ano_mes)
    
    # print(ps_frame.columns)
    # ['versao_biblioteca', 'licenca_biblioteca', 'nome_repositorio', 
    # 'gerenciador_biblioteca', 'nome_biblioteca', 'anomes']
    ps_frame.show(truncate=False, n=4)
    
    tbl_repositorio_frame = create_micro_frame_with_domain('nome_repositorio', ps_frame)
    tbl_biblioteca_frame = create_micro_frame_with_domain('nome_biblioteca', ps_frame)
    tbl_gerenciador_frame = create_micro_frame_with_domain('gerenciador_biblioteca', ps_frame)
    tbl_licenca_frame = create_micro_frame_with_domain('licenca_biblioteca', ps_frame)
    
    table_name = 'tbl_biblioteca'
    
    # inser_data_db(database_sor, table_name, tbl_biblioteca_frame, gluecontext)
    # tbl_biblioteca_frame.show(truncate=False, n=20)
    # tables = list_tables(database_sor)
    # print(tables)
    # # [' tbl_conformidade_open_source', 'tbl_biblioteca',
    # # 'tbl_gerenciador_biblioteca', 'tbl_licenca', 'tbl_repositorio']
    # #
    
    ps_frame_from_dy = view_table_glue(gluecontext, database_sor, table_name)
    try:
        max_id = ps_frame_from_dy.selectExpr(f"max(biblioteca_id) as max_id").first()["max_id"]
    except AnalysisException:
        max_id = 0
    try:
        frame_new = create_hash_validate(frame=tbl_biblioteca_frame, columns= ['nome_biblioteca'])
        frame_glue = create_hash_validate(frame=ps_frame_from_dy, columns= ['nome_biblioteca'])
        
        frame_to_update = frame_insert(left_frame=frame_new, right_frame=frame_glue)
        print('Frame frame_to_update line[272]:')
        frame_to_update.printSchema()
    except AnalysisException:
        frame_to_update = tbl_biblioteca_frame
    
    frame_to_update = frame_to_update.withColumn(
        'biblioteca_id',  F.row_number().over(
            Window.orderBy(frame_to_update.nome_biblioteca.asc()))+ max_id)
    print('Frame frame_to_update line[278]:')
    frame_to_update.printSchema()
    
    inser_data_db(database_sor, table_name, frame_to_update, gluecontext)
        

        
    # print(ps_frame_from_dy.columns)
    # ps_frame_from_dy.show()
    # ps_frame_from_dy.printSchema()
    
    
    # ps_frame_from_dy.createOrReplaceTempView(f"vw_{table_name}")
    
    # psq_frame_tb = sparkSession.sql(f'select * from vw_{table_name}')
    # psq_frame_tb.show(truncate=False, n=20)
    # # for table in tables:
    # #     view_table_glue(gluecontext, database_sor, table)
    
    
    
    
    # repositorio_frame = ps_frame.select().withColumn("repositorio_id", F.monotonically_increasing_id())
    # gerenciadores_frame = ps_frame.withColumn("gerenciador_id", F.monotonically_increasing_id())
    # licencas_frame = ps_frame.withColumn("licenca_id", F.monotonically_increasing_id())
    
    # Example usage
    # schemas = get_all_schemas(database_sor)
    
    # print(f"Tables in database '{database_sor}': {tables}")
    # print(f"Tables in schemas '{database_sor}': {schemas}")
    
    # Tables in database 'conformidade_open_source': ['tbl_biblioteca', 'tbl_conformidade_open_source', 'tbl_gerenciador_biblioteca', 'tbl_licenca', 'tbl_repositorio']
    # Tables in schemas 'conformidade_open_source':
    #     {
    #         'tbl_biblioteca': [('biblioteca_id', 'int'), ('nome_biblioteca', 'string')],
    #         'tbl_conformidade_open_source': [('repositorio_id', 'int'), ('gerenciador_id', 'int'), ('biblioteca_id', 'int'), ('versao_biblioteca', 'string'), ('anomesdia', 'string')],
    #         'tbl_gerenciador_biblioteca': [('gerenciador_id', 'int'), ('nome_gerenciador_biblioteca', 'string')], 'tbl_licenca': [('licenca_id', 'int'), ('nome_licenca', 'string')], 
    #         'tbl_repositorio': [('repositorio_id', 'int'), ('nome_repositorio', 'string')]
            
    #     }

        
    logger.info('===============   FINISH JOB   ================')
    
    job_init.commit()

if __name__ == '__main__':
    main()
    

