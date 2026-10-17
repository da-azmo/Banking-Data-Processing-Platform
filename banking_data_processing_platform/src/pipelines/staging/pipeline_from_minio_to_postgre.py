import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent / "core"))
sys.path.append(str(Path(__file__).parent.parent.parent / "infrastructure" / "spark"))
sys.path.append(str(Path(__file__).parent.parent.parent / "infrastructure" / "database"))
sys.path.append(str(Path(__file__).parent.parent.parent / "jobs" / "staging"))

from config import get_config
from session import get_spark_session
from postgre import get_jdbc_options
from extract_from_minio_raw_to_postgre_staging import get_extract_from_minio_raw_to_postgre_staging
from transform_from_minio_raw_to_postgre_staging import get_transform_from_minio_raw_to_postgre_staging
from load_from_minio_raw_to_postgre_staging import get_load_from_minio_raw_to_postgre_staging

CONFIG_ODS = get_config("dwh_config", "ods")
JDBC_OPTIONS = get_jdbc_options()

def pipeline_staging():

    spark = get_spark_session()

    dataframes = get_extract_from_minio_raw_to_postgre_staging(spark, CONFIG_ODS)

    #transformed_dataframes = get_transform_from_minio_raw_to_postgre_staging(dataframes, CONFIG_ODS)

    #get_load_from_minio_raw_to_postgre_staging(transformed_dataframes, JDBC_OPTIONS)

if __name__ == "__main__":
    pipeline_staging()
