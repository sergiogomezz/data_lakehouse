import sys
import os
import uuid

from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import lit, col, count, countDistinct, when, isnan, isnull, asc, desc
from pyspark.ml.feature import Imputer
import pyspark.sql.functions as F
# import boto3
# import pandas as pd
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions


# SETTING GLOBAL VARIABLES
TABLE_NAME = 'telemetrydatatable'
BASE_PATH = f's3://datalake-bucket-178395378273'
FINAL_BASE_PATH = '{BASE_PATH}/{TABLE_NAME}'.format(
    BASE_PATH=BASE_PATH, TABLE_NAME=TABLE_NAME)
PARTITION_FIELD = 'device_id'
RECORD_KEY = ['device_id', 'timestamp']
DATABASE_NAME = 'telemetry_glue_database' 

# CREATING SPARK SESSION
# CHECK THAT THE VERSIONS ARE UP TO DATE
def create_spark_session():
    spark = SparkSession.builder \
        .appName("Hudi-app") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config('spark.kryo.registrator', 'org.apache.spark.HoodieSparkKryoRegistrar') \
        .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
        .config(
            'spark.jars.packages',
            'org.apache.hudi:hudi-spark3-bundle_2.12:0.14.1'
        ) \
        .getOrCreate()
    return spark

# CREATING SESSIONS AND CONTEXTS
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
spark = create_spark_session()
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read data
# THE CONNECTOR TO READ FROM DYNAMODB WITH GLUE IS STILL MISSING
AmazonDynamoDB_node1715667391318 = glueContext.create_dynamic_frame.from_catalog(
    database="telemetry_glue_database",
    table_name="telemetrydatatable",
    transformation_ctx="AmazonDynamoDB_node1715667391318")

# Convert to a Spark DataFrame
df = AmazonDynamoDB_node1715667391318.toDF()


# SETTING HUDI OPTIONS 
hudi_options = {
    'hoodie.table.name': TABLE_NAME,
    'hoodie.datasource.write.storage.type': 'COPY_ON_WRITE', # DECIDE IF COW OR MOR WILL BE USED
    'hoodie.datasource.write.recordkey.field': ','.join(RECORD_KEY),
    'hoodie.datasource.write.table.name': TABLE_NAME,
    'hoodie.datasource.write.operation': 'upsert', # CHANGE TO BULK INSERT IF MASSIVE DATA WILL BE INSERTED
    'hoodie.datasource.write.partitionpath.field': PARTITION_FIELD,

    'hoodie.datasource.hive_sync.enable': 'true',
    'hoodie.datasource.hive_sync.mode': 'hms',
    'hoodie.datasource.hive_sync.sync_as_datasource': 'false',
    'hoodie.datasource.hive_sync.database': DATABASE_NAME,
    'hoodie.datasource.hive_sync.use_jdbc': 'false',
    'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
    'hoodie.datasource.write.hive_style_partitioning': 'true'
}

# GET THE DATA AND POPULATE THE HUDI TABLE
df.write.format('hudi') \
    .options(**hudi_options) \
    .mode('overwrite') \
    .save(FINAL_BASE_PATH)

# DELETE THOSE ENTRIES WHERE DEVICE_ID OR TIMESTAMP ARE MISSING VALUES
filtered_nullKeysDF = spark.read.format('hudi').load(FINAL_BASE_PATH) \
    .filter(col('device_id').isNull() | col('timestamp').isNull())
hudi_delete_options = hudi_options.copy()
hudi_delete_options['hoodie.datasource.write.operation'] = 'delete'
filtered_nullKeysDF.write.format('hudi') \
    .options(**hudi_delete_options) \
    .mode('append') \
    .save(FINAL_BASE_PATH)

# DELETE THOSE ENTRIES WITH MORE OF THE HALF EMPTY COLUMNS
moreMissingDF = spark.read.format('hudi').load(FINAL_BASE_PATH)
# excluding both keys
excluded_columns = ['device_id', 'timestamp']
to_count_columns = [col for col in moreMissingDF.columns if col not in excluded_columns]
# setting threshold in half of the columns
num_col = len(moreMissingDF.columns)
threshold = num_col // 2
# counting missing values and implement a new DF
null_counts = sum(col(c).isNull().cast("int").alias(c) for c in to_count_columns)
filtered_moreMissingDF = moreMissingDF.filter(null_counts > threshold)
filtered_moreMissingDF.write.format('hudi') \
    .options(**hudi_delete_options) \
    .mode('append') \
    .save(FINAL_BASE_PATH)

# TO IMPUTE NULL DATA
df = spark.read.format('hudi').load(FINAL_BASE_PATH)
numeric_columns = ['temperature', 'pressure', 'humidity']
imputer_mean = Imputer(inputCols=numeric_columns, outputCols=numeric_columns)
imputed_df = imputer_mean.setStrategy('mean').fit(df).transform(df)
imputed_df.write.format('hudi') \
    .options(**hudi_options) \
    .mode('append') \
    .save(FINAL_BASE_PATH)
