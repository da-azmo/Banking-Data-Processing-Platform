from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

load_dotenv()

def get_spark_session():
    
    spark = ( SparkSession.builder
        .appName("SparkApp")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_S3_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_S3_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_S3_ENDPOINT"))
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.apache.hadoop:hadoop-aws:3.5.0")
    ).getOrCreate()

    return spark