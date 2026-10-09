import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "core"))
sys.path.append(str(Path(__file__).parent.parent / "infrastructure" / "spark"))
sys.path.append(str(Path(__file__).parent.parent / "jobs" / "raw" / "csv"))

from metadata_s3 import get_s3_config
from session import get_spark_session
from extract_from_csv_postgre_to_minio_s3 import get_extract_from_csv_postgre_to_minio_s3
from load_from_csv_postgre_to_minio_s3 import get_load_from_csv_postgre_to_minio_s3

CONFIG_CSV = get_s3_config()

def pipeline_csv():

    spark = get_spark_session()

    df = get_extract_from_csv_postgre_to_minio_s3(spark, CONFIG_CSV)

    get_load_from_csv_postgre_to_minio_s3(df, CONFIG_CSV)

    spark.stop()

if __name__ == "__main__":
    pipeline_csv()