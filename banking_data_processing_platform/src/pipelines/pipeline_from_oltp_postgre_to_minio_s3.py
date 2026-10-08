from pyspark.sql import SparkSession
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent / 'jobs' / 'raw'))
sys.path.append(str(Path(__file__).parent.parent / 'infrastructure' / 'spark'))

from extract_from_oltp_postgre_to_minio_s3 import get_extract_from_oltp_postgre_to_minio_s3
from load_from_oltp_postgre_to_minio_s3 import get_load_from_oltp_postgre_to_minio_s3
from session import get_spark_session

def pipeline():

    spark = get_spark_session()

    df = get_extract_from_oltp_postgre_to_minio_s3(spark)

    query = get_load_from_oltp_postgre_to_minio_s3(df)

    query.awaitTermination()

    spark.stop()

if __name__ == "__main__":
    pipeline()