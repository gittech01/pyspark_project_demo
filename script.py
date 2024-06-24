import sys
import boto3
import logging

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from awsglue.context import GlueContext
from awsglue.job import Job


# sys.stdout = Unbuffered(sys.stdout)

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

logger.info('often makes a very good meal of %s', 'visiting tourists')


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def get_envs():
    return getResolvedOptions(
            sys.argv,
            ['JOB_NAME']
        )


def read_files_infos_complience(glue_context, s3_bucket_stage):
    print(f"Start read files in s3_bucket_stage: {s3_bucket_stage}")
    try:
        dynamicframe = (
            glue_context
            .create_dynamic_frame
            .from_options(
                connection_type='s3',
                connection_options={
                    'paths': [s3_bucket_stage],
                    'recurse': True
                },
                format='json',
                format_options={"multiline": True}, 
            )
        )
    except Exception as err:
        print(f"Error: {error}")
        
    return dynamicframe
    

def select_explode_columns_spark_frame(dataframe):
    
    select_explode_dataframe = (
            dataframe
            .select(
                F.col("name").alias("new_name"),
                F.explode(F.col("packages")).alias("pacotes")
            )
            .select("new_name", "pacotes.*")
        )
        
    return select_explode_dataframe, select_explode_dataframe.columns


def create_select_columns_spark_frame(dataframe, cols: list):
    COLS = ["name", "versionInfo", "licenseConcluded"]
    
    for col in COLS:
        new_name_col = (
                    "new_biblioteca" if col == COLS[0]
            else "versao_biblioteca" if col == COLS[1]
            else "licenca_biblioteca"
        )
        if col not in cols:
            dataframe = dataframe.withColumn(new_name_col, F.lit(None))
            continue
        dataframe = dataframe.withColumnRenamed(col, new_name_col)

    return dataframe
    

def transform_spark_frame(dataframe):
    
    dataframe, cols = select_explode_columns_spark_frame(dataframe)
    dataframe = create_select_columns_spark_frame(dataframe, cols)

    dataframe = dataframe.select(
                    F.col("new_name")
                    , F.col("new_biblioteca")
                    , F.col("versao_biblioteca")
                    , F.col("licenca_biblioteca")
                )
    
    final_dataframe = dataframe.filter(dataframe.new_name != dataframe.new_biblioteca)
    final_dataframe = final_dataframe.withColumn("nome_repositorio", F.split(F.col("new_name"), '/')[1])
    final_dataframe = final_dataframe.withColumn("gerenciador_biblioteca", F.split(F.col("new_biblioteca"), ':')[0])
    final_dataframe = final_dataframe.withColumn("nome_biblioteca", F.split(F.col("new_biblioteca"), ':')[1])

    final_dataframe = (
            final_dataframe.
            select(*[col for col in final_dataframe.columns if col not in ('new_name','new_biblioteca')])
        )
    
    return final_dataframe
    
logger.info('     init     ')


s3_bucket_stage = "s3://aws-glue-study-ingest-data/sbom/"

dataframe_glue = read_files_infos_complience(glueContext, s3_bucket_stage)
# dataframe_glue.show(5)

dataframe_pyspark = dataframe_glue.toDF()
dataframe_pyspark = transform_spark_frame(dataframe_pyspark)
dataframe_pyspark.show(truncate=False, n=50)
# dataframe_pyspark.count()

job.commit()