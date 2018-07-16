import os
import sys
import boto3

from awsglue.job import Job
from awsglue.transforms import *
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

import pyspark.sql.functions as F
from pyspark.sql import Row, Window, SparkSession
from pyspark.sql.types import *
from pyspark.conf import SparkConf
from pyspark.context import SparkContext


args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
spark._jsc.hadoopConfiguration().set("parquet.enable.summary-metadata", "false")

## Read in data by pointing to its's table name in Glue Data Catalog
schema = StructType() \
  .add('source', StringType()) \
  .add('type', StringType()) \
  .add('data', StringType()) \
  .add('ts', StringType())

src = spark.read.load('s3://demoanalyticsapp-output/*/*/*/*/', format='parquet', schema=schema)

## Build out some new columns with data extracted from the JSON string

df = src \
  .withColumn('n_ts', F.unix_timestamp('ts', "yyyy-MM-dd'T'hh:mm:ss").cast('timestamp')) \
  .withColumn('agerange', \
    F.from_json( \
      F.get_json_object('data', '$.facedetails[*].agerange'), \
      StructType().add('high', IntegerType()).add('low', IntegerType()) \
    ) \
  ) \
  .withColumn('smile', \
    F.from_json( \
      F.get_json_object('data', '$.facedetails[*].smile'), \
      StructType().add('confidence', DoubleType()).add('value', BooleanType()) \
    ) \
  ) \
  .withColumn('eyeglasses', \
    F.from_json( \
      F.get_json_object('data', '$.facedetails[*].eyeglasses'), \
      StructType().add('confidence', DoubleType()).add('value', BooleanType()) \
    ) \
  ) \
.withColumn('sunglasses', \
    F.from_json( \
      F.get_json_object('data', '$.facedetails[*].sunglasses'), \
      StructType().add('confidence', DoubleType()).add('value', BooleanType()) \
    ) \
  ) \
.withColumn('gender', \
    F.from_json( \
      F.get_json_object('data', '$.facedetails[*].gender'), \
      StructType().add('confidence', DoubleType()).add('value', BooleanType()) \
    ) \
  ) \
.withColumn('beard', \
    F.from_json( \
      F.get_json_object('data', '$.facedetails[*].beard'), \
      StructType().add('confidence', DoubleType()).add('value', BooleanType()) \
    ) \
  ) \
.withColumn('mustache', \
    F.from_json( \
      F.get_json_object('data', '$.facedetails[*].mustache'), \
      StructType().add('confidence', DoubleType()).add('value', BooleanType()) \
    ) \
  ) \
.withColumn('eyesopen', \
    F.from_json( \
      F.get_json_object('data', '$.facedetails[*].eyesopen'), \
      StructType().add('confidence', DoubleType()).add('value', BooleanType()) \
    ) \
  ) \
.withColumn('mouthopen', \
    F.from_json( \
      F.get_json_object('data', '$.facedetails[*].mouthopen'), \
      StructType().add('confidence', DoubleType()).add('value', BooleanType()) \
    ) \
  ) \
.drop('ts') \
.withColumnRenamed('n_ts', 'ts') \
.withColumn('year', F.year('ts')) \
.withColumn('month', F.month('ts'))

## Sometimes we need to distribute the data based on a specific column, higher cardinality is better.
## To see the number of spark partitions being used: df.rdd.getNumPartitions()
df = df.repartition('ts')

## Finally write the data back out to S3 in partitioned Parquet format
## maxRecordsPerFile is recommended over the old method of using coalesce()
df \
  .withColumn('smiling', df.smile.value) \
  .write \
  .option('maxRecordsPerFile', 1000) \
  .partitionBy('year', 'month', 'smiling') \
  .mode('overwrite') \
  .parquet('s3://bucket/prefix')