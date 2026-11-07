from pyspark.sql import SparkSession
import sys
from pathlib import Path

sys.path.append(Path(__file__).parent.parent.parent.parent / "core")

from metadata import get_dm_s3_path

def get_extract___dim_dates_from_minio(spark: SparkSession, CONFIG_DM: dict):

    s3_path = get_dm_s3_path(CONFIG_DM).format(table_name="dim_dates")

    df = spark.read.parquet(s3_path)

    return df