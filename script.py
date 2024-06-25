import sys
import boto3
import logging
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from awsglue.context import GlueContext
from awsglue.job import Job


logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

logger.info('often makes a very good meal of %s', 'visiting tourists')


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
    

def view_table_glue(glue_context, database, table_name):
    dynamic_frame = (
        glue_context
        .create_dynamic_frame
        .from_catalog(
            database=database, 
            table_name=table_name
        )
    )
    frame = dynamic_frame.toDF()
    frame.createOrReplaceTempView(f"view_{table_name}")


def main():
    
    args = get_envs()
    
    gluecontext, sparkSession = init_context()
    job_init = init_job(gluecontext, args)
    glue_client = init_glue_client()
    
    ano_mes = get_anomes()
    bucket_stage = args['BUCKET_STAGE']
    bucket_sor = args['BUCKET_SOR']
    database_sor = args['DATABASE_SOR']
    
    pyspark_frame = transform_frame(sparkSession, bucket_stage, ano_mes)
    
    print(pyspark_frame.columns)    # 	['versao_biblioteca', 'licenca_biblioteca', 'nome_repositorio', 'gerenciador_biblioteca', 'nome_biblioteca', 'anomes']
    pyspark_frame.show(truncate=False, n=4)
    # pyspark_frame.printSchema()
    
    tbl_repositorio = create_micro_frame_with_domain('nome_repositorio', pyspark_frame)
    tbl_biblioteca = create_micro_frame_with_domain('nome_biblioteca', pyspark_frame)
    tbl_gerenciador = create_micro_frame_with_domain('gerenciador_biblioteca', pyspark_frame)
    tbl_licenca = create_micro_frame_with_domain('licenca_biblioteca', pyspark_frame)
    
    tbl_biblioteca.show(truncate=False, n=20)
    tables = list_tables(database_sor)
    
    # corrigir aqui
    for table in tables:
        view_table_glue(gluecontext, database_sor, table)
    
    
    
    
    # repositorio_frame = pyspark_frame.select().withColumn("repositorio_id", F.monotonically_increasing_id())
    # gerenciadores_frame = pyspark_frame.withColumn("gerenciador_id", F.monotonically_increasing_id())
    # licencas_frame = pyspark_frame.withColumn("licenca_id", F.monotonically_increasing_id())
    
    # Example usage
    # schemas = get_all_schemas(database_sor)
    
    # print(f"Tables in database '{database_sor}': {tables}")
    # print(f"Tables in schemas '{database_sor}': {schemas}")
        
    job_init.commit()
    

if __name__ == '__main__':
    main()
    

