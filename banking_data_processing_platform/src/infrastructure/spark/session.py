from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / 'storage'))

from minio import get_s3_connection_options

load_dotenv()

def get_spark_session():

    s3_connection_options = get_s3_connection_options()
    
    spark = ( SparkSession.builder
        .appName("SparkApp")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.ui.port", "4040")
        .config("spark.ui.bindAddress", "0.0.0.0")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", s3_connection_options.get('access_key'))
        .config("spark.hadoop.fs.s3a.secret.key", s3_connection_options.get('secret_key'))
        .config("spark.hadoop.fs.s3a.endpoint", s3_connection_options.get('endpoint'))
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.apache.hadoop:hadoop-aws:3.5.0,org.postgresql:postgresql:42.7.13")
    ).getOrCreate()

    return spark