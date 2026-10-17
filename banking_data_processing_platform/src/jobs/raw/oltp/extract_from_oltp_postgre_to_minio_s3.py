from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent.parent / "core"))

from metadata import get_debezium_oltp_tables_names

load_dotenv()

def get_extract_from_oltp_postgre_to_minio_s3(spark: SparkSession, config: dict):
    dataframes = {}
    tables = get_debezium_oltp_tables_names(config)

    for table in tables:
    
        df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", os.getenv("KAFKA_PORT_EXTERNAL"))
            .option("subscribe", table)
            .option("startingOffsets", "earliest")
            .load()
        )

        dataframes[table] = df

    return dataframes