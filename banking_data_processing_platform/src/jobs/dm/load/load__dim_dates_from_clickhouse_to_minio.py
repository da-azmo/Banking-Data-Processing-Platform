from pyspark.sql import DataFrame
import sys
from pathlib import Path

sys.path.append(Path(__file__).parent.parent.parent.parent / "core")

from metadata import get_dm_s3_path

def get_load__dim_dates_from_clickhouse_to_minio(df: DataFrame, CONFIG_DM: dict):

    s3_path = get_dm_s3_path(CONFIG_DM).format(table_name="dim_dates")

    (
        df.write
            .format("parquet")
            .mode("overwrite")
            .save(s3_path)
    )