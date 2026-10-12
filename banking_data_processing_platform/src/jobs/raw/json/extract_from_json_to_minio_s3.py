from pyspark.sql import SparkSession
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent.parent / "core"))

from metadata import get_all_tables

def get_extract_from_json_to_minio_s3(spark: SparkSession, config: dict):
    dataframes = {}
    tables = get_all_tables(config)

    for table in tables:
        table_name = table.get("name")
        file_path = table.get("file_path")

        df = (spark.read
            .format("json")
            .option("multiLine", "true")
            .load(file_path))

        dataframes[table_name] = df

    return dataframes