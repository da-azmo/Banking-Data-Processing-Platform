import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent / "core"))
sys.path.append(str(Path(__file__).parent.parent.parent / "infrastructure" / "spark"))
sys.path.append(str(Path(__file__).parent.parent.parent / "jobs" / "raw" / "api"))

from config import get_config
from session import get_spark_session
from extract_from_api_to_minio_s3 import get_extract_from_api_to_minio_s3
from load_from_api_to_minio_s3 import get_load_from_api_to_minio_s3

CONFIG_API = get_config("sources_for_raw", "api")

def pipeline_api():

    spark = get_spark_session()

    df = get_extract_from_api_to_minio_s3(spark, CONFIG_API)

    get_load_from_api_to_minio_s3(df, CONFIG_API)

    spark.stop()

if __name__ == "__main__":
    pipeline_api()