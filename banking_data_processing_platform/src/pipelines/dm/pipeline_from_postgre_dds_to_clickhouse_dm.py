import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent / "core"))
sys.path.append(str(Path(__file__).parent.parent.parent / "infrastructure" / "spark"))
sys.path.append(str(Path(__file__).parent.parent.parent / "infrastructure" / "database"))
sys.path.append(str(Path(__file__).parent.parent.parent / "jobs" / "dm" / "extract"))
sys.path.append(str(Path(__file__).parent.parent.parent / "jobs" / "dm" / "transform"))

from config import get_config
from session import get_spark_session
from postgre import get_jdbc_POSTGRE_dds_options
from clickhouse import get_jdbc_CLICKHOUSE_options
from extract_from_postgre_dds_to_clickhouse_dm import get_extract_from_postgre_dds_to_clickhouse_dm
from transform_from_postgre_dds_to_clickhouse_dm import get_transform_from_postgre_dds_to_clickhouse_dm

CONFIG_DDS = get_config("dwh_config", "dds")
CONFIG_DM = get_config("dwh_config", "dm_for_transform")
CONFIG_DM_RESULT_COLUMNS = get_config("dwh_config", "dm_for_result")
POSTGRE_JDBC_OPTIONS = get_jdbc_POSTGRE_dds_options()
CLICKHOUSE_JDBC_OPTIONS = get_jdbc_CLICKHOUSE_options()

def pipelines_dm():

    spark = get_spark_session()

    dfs = get_extract_from_postgre_dds_to_clickhouse_dm(spark, CONFIG_DDS, CONFIG_DM, POSTGRE_JDBC_OPTIONS, CLICKHOUSE_JDBC_OPTIONS)
    
    get_transform_from_postgre_dds_to_clickhouse_dm(spark, dfs, CONFIG_DM, CONFIG_DM_RESULT_COLUMNS)
    
    spark.stop()

if __name__ == "__main__":
    pipelines_dm()