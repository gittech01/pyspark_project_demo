import sys
import boto3
import logging
from datetime import datetime
from functools import wraps

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F, Window
from pyspark.sql.utils import AnalysisException
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


# Configuração básica do logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d]: %(message)s'
)
logger = logging.getLogger()


def log_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f'Starting function execution: {func.__name__}')
        try:
            result = func(*args, **kwargs)
            logger.info(f'Function {func.__name__} executed successfully')
            return result
        except Exception as e:
            logger.error(f'Error executing function {func.__name__}: {e}')
            raise
    return wrapper


@log_decorator
def client_glue():
    client = boto3.client('glue')
    return client


@log_decorator
def list_tables(client_glue, database):
    table_names = []
    try:
        response = client_glue.get_tables(DatabaseName=database)
        tables = response['TableList']
        table_names = [table['Name'] for table in tables]
    except Exception as e:
        print(f"Error fetching tables: {e}")
    return table_names


@log_decorator
def get_table_schema(client_glue, database, table):
    try:
        response = client_glue.get_table(DatabaseName=database, Name=table)
        table = response['Table']
        schema = table['StorageDescriptor']['Columns']
        
        return schema
    except Exception as e:
        print(f"Error fetching schema for table {table_name}: {e}")
        return []


@log_decorator
def get_all_schemas(client_glue, database):
    tables = list_tables(client_glue, database)
    schemas = {}
    for table in tables:
        schemas[table] = get_table_schema(
                            client_glue=client_glue, database=database, table=table
                         )
    else:
        mappings_table = {}
        for table, columns in schemas.items():
            mappings_columns = [(col['Name'], col['Type']) for col in columns]
            mappings_table[table] = mappings_columns
        return mappings_table


@log_decorator
def get_envs():
    return getResolvedOptions(sys.argv,[
            'JOB_NAME', 'BUCKET_STAGE'
            , 'BUCKET_SOR', 'DATABASE_SOR'
        ])


@log_decorator
def init_context():
    spark_context = SparkContext()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session
    
    return glue_context, spark


@log_decorator
def init_job(glue_context, args):
    job_init = Job(glue_context)
    job_init.init(args['JOB_NAME'], args)
    return job_init
    

@log_decorator
def get_anomesdia():
    return datetime.now().strftime('%Y%m%d')
    

@log_decorator
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
    

@log_decorator
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


@log_decorator
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
    

@log_decorator
def transform_frame(spark_session, bucket_stage, anomesdia):
    frame = read_raw_info(spark_session, bucket_stage)
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
            .withColumn('anomesdia', F.lit(anomesdia))
        )
    final_frame = final_frame.fillna({'licenca_biblioteca': 'NOASSERTION'})
    final_frame = final_frame.distinct()
    
    return final_frame
    

@log_decorator
def create_micro_frame_with_domain(column_name, frame):
    frame_r = frame.select(F.col(column_name)).distinct()
    return frame_r
    

@log_decorator
def view_table_glue(glue_context, database, table):
    dynamic_frame = (
        glue_context
        .create_dynamic_frame
        .from_catalog(
            database=database, 
            table_name=table
        )
    )
    
    ps_frame = dynamic_frame.toDF()
    return ps_frame
    

@log_decorator
def create_hash_validate(frame, columns: list):
    hash_valiadte = (
        frame.withColumn(
            'id_check',
            F.md5(F.concat_ws(':', *columns))
        ))
    return hash_valiadte


@log_decorator    
def frame_insert(left_frame, right_frame, how_:str='left_anti'):
    result = left_frame.join(right_frame, on='id_check', how=how_)
    result = result.select(*[col for col in result.columns if col not in ('id_check',)])
    return result
    

@log_decorator
def inser_data_db(
    database,
    table,
    frame,
    glue_context,
    partition: str = None
):
    
    dynamic_frame = DynamicFrame.fromDF(frame, glue_context, "dynamic_frame")
    glue_context.write_dynamic_frame.from_catalog(
        frame=dynamic_frame,
        database=database,
        table_name=table,
        additional_options={
            "partitionKeys": [partition] if partition else []
        },
        transformation_ctx="dataSink"
    )

    

def main():
    
    logger.info('===============   START JOB   ================')
    args = get_envs()
    
    gluecontext, sparkSession = init_context()
    job = init_job(glue_context=gluecontext, args=args)
    glueclient = client_glue()
    
    anomesdia = get_anomesdia()
    bucket_stage = args['BUCKET_STAGE']
    bucket_sor = args['BUCKET_SOR']
    database_sor = args['DATABASE_SOR']
    
    ps_frame = transform_frame(
                    spark_session=sparkSession,
                    bucket_stage=bucket_stage,
                    anomesdia=anomesdia
                )
    
    # print(ps_frame.columns)
    # ['versao_biblioteca', 'licenca_biblioteca', 'nome_repositorio', 
    # 'gerenciador_biblioteca', 'nome_biblioteca', 'anomes']
    ps_frame.show(truncate=False, n=4)
    mappings_table = get_all_schemas(client_glue=glueclient, database=database_sor)

    tb_biblioteca_frame = create_micro_frame_with_domain('nome_biblioteca', ps_frame)
    table_name_sor = 'tb_biblioteca'
    
    # inser_data_db(database_sor, table_name, tbl_biblioteca_frame, gluecontext)
    # tbl_biblioteca_frame.show(truncate=False, n=20)
    # tables = list_tables(database_sor)
    # print(tables)
    # # [' tbl_conformidade_open_source', 'tbl_biblioteca',
    # # 'tbl_gerenciador_biblioteca', 'tbl_licenca', 'tbl_repositorio']
    # #
    
    ps_frame_from_dy = view_table_glue(
                            glue_context=gluecontext,
                            database=database_sor,
                            table=table_name_sor
                        )
    try:
        max_id = ps_frame_from_dy.selectExpr(f"max(biblioteca_id) as max_id").first()["max_id"]
    except AnalysisException:
        max_id = 0
    try:
        if max_id:
            frame_new = create_hash_validate(frame=tb_biblioteca_frame, columns= ['nome_biblioteca'])
            frame_glue = create_hash_validate(frame=ps_frame_from_dy, columns= ['nome_biblioteca'])
            
            frame_to_update = frame_insert(left_frame=frame_new, right_frame=frame_glue)
        frame_to_update = tb_biblioteca_frame
    except AnalysisException:
        frame_to_update = tb_biblioteca_frame
    
    frame_to_update = frame_to_update.withColumn(
        'biblioteca_id',  F.row_number().over(
            Window.orderBy(frame_to_update.nome_biblioteca.asc()))+ max_id)

    frame_to_update.show(truncate=False, n=4)
    
    inser_data_db(
        database=database_sor,
        table=table_name_sor,
        frame=frame_to_update,
        glue_context=gluecontext,
    )
    
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

        
    logger.info('===============   START JOB   ================')
    
    job.commit()

if __name__ == '__main__':
    main()
