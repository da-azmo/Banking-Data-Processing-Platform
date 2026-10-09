from pyspark.sql import SparkSession
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent.parent / "core"))

from metadata_s3 import get_s3_tables

def get_extract_from_csv_postgre_to_minio_s3(spark: SparkSession, config: dict):

    dataframes = {}
    s3_tables = get_s3_tables(config)

    for table in s3_tables:
        
        df = (
            spark.read
            .format("csv")
            .option("header", "true")
            .load(table.get("file_path"))
        )

        dataframes[table.get("name")] = df

    return dataframes