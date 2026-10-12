import requests
from pyspark.sql import SparkSession
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent.parent / 'core'))

from metadata import get_all_tables

def get_extract_from_api_to_minio_s3(spark: SparkSession, api_config: dict):

    dataframes = {}

    tables = get_all_tables(api_config)

    for table in tables:

        table_name = table.get("name")
        url = table.get("url")
        params = table.get("params")

        response = requests.get(url, params=params, timeout=30)
        # проверка успешности запроса
        response.raise_for_status()

        records = response.json()

        if not records:
            raise ValueError("Currency API returned no records")

        df = spark.createDataFrame(records)

        dataframes[table_name] = df

    return dataframes