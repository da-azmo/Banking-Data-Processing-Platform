from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

load_dotenv()

def get_extract_from_oltp_postgre_to_minio_s3(spark: SparkSession):
    
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_PORT_EXTERNAL"))
        .option("subscribe", "banking-data-processing-platform-source.sources.clients")
        .option("startingOffsets", "earliest")
        .load()
    )

    return df