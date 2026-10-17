import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent / "core"))
sys.path.append(str(Path(__file__).parent.parent.parent / "infrastructure" / "spark"))
sys.path.append(str(Path(__file__).parent.parent.parent / "jobs" / "raw" / "json"))

from config import get_config
from session import get_spark_session
from extract_from_json_to_minio_s3 import get_extract_from_json_to_minio_s3
from load_from_json_to_minio_s3 import get_load_from_json_to_minio_s3

CONFIG_JSON = get_config("sources_for_raw", "json")

def pipeline_json():

    spark = get_spark_session()

    df = get_extract_from_json_to_minio_s3(spark, CONFIG_JSON)

    get_load_from_json_to_minio_s3(df, CONFIG_JSON)

    print(f"sparkUI: {spark.sparkContext.uiWebUrl}")
    input("Нажмите Enter для продолжения")

    spark.stop()

if __name__ == "__main__":
    pipeline_json()