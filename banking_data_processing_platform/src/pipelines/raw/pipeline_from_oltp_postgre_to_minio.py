from pyspark.sql import SparkSession
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent.parent / 'core'))
sys.path.append(str(Path(__file__).parent.parent.parent / 'jobs' / 'raw' / 'oltp'))
sys.path.append(str(Path(__file__).parent.parent.parent / 'infrastructure' / 'debezium'))
sys.path.append(str(Path(__file__).parent.parent.parent / 'infrastructure' / 'spark'))

from config import get_config, get_debezium_connector_config
from postgre_connector import check_debezium_connector
from extract_from_oltp_postgre_to_minio_s3 import get_extract_from_oltp_postgre_to_minio_s3
from load_from_oltp_postgre_to_minio_s3 import get_load_from_oltp_postgre_to_minio_s3
from session import get_spark_session

CONFIG_CONNECTOR = get_debezium_connector_config("postgre-source")
CONFIG_POSTGRE = get_config("sources_for_raw", "postgre")

def pipeline_oltp():

    check_debezium_connector(CONFIG_CONNECTOR)

    spark = get_spark_session()

    dataframes = get_extract_from_oltp_postgre_to_minio_s3(spark, CONFIG_CONNECTOR)
    queries = get_load_from_oltp_postgre_to_minio_s3(dataframes, CONFIG_POSTGRE)

    print(f"sparkUI: {spark.sparkContext.uiWebUrl}")
    input("Нажмите Enter для продолжения")

    spark.awaitAnyTermination()

    spark.stop()

if __name__ == "__main__":
    pipeline_oltp()