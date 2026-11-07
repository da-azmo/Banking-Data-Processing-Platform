import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent / "core"))
sys.path.append(str(Path(__file__).parent.parent.parent / "infrastructure" / "spark"))
sys.path.append(str(Path(__file__).parent.parent.parent / "infrastructure" / "database"))
sys.path.append(str(Path(__file__).parent.parent.parent / "jobs" / "dm" / "extract"))
sys.path.append(str(Path(__file__).parent.parent.parent / "jobs" / "dm" / "load"))

from config import get_config
from session import get_spark_session
from clickhouse import get_jdbc_CLICKHOUSE_options
from extract__dim_dates_from_clickhouse_to_minio import get_extract__dim_dates_from_clickhouse_to_minio
from load__dim_dates_from_clickhouse_to_minio import get_load__dim_dates_from_clickhouse_to_minio

CONFIG_DM = get_config("dwh_config", "dm_for_transform")
CLICKHOUSE_JDBC_OPTIONS = get_jdbc_CLICKHOUSE_options()

def pipelines_dm():

    spark = get_spark_session()

    dfs = get_extract__dim_dates_from_clickhouse_to_minio(spark, CLICKHOUSE_JDBC_OPTIONS)

    get_load__dim_dates_from_clickhouse_to_minio(dfs, CONFIG_DM)
    
    spark.stop()

if __name__ == "__main__":
    pipelines_dm()