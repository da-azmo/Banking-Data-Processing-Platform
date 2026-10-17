from pyspark.sql import SparkSession
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent / "core"))

from metadata import get_all_tables, get_s3_path

def get_extract_from_minio_raw_to_postgre_staging(spark: SparkSession, config: dict):
    dataframes = {}
    tables = get_all_tables(config)

    for table in tables:

        table_name = table.get("name")
        
        if table_name == "exchange_rates":
            s3_path = f"s3a://banking-data-processing-platform/raw/api/sources/{table_name}/2026/10/16/21"
        elif table_name == "payments":
            s3_path = f"s3a://banking-data-processing-platform/raw/json/sources/{table_name}/2026/10/16/21"
        elif table_name == "transactions":
            s3_path = f"s3a://banking-data-processing-platform/raw/csv/sources/{table_name}/2026/10/16/21"
        else:
            s3_path = f"s3a://banking-data-processing-platform/raw/postgre/sources/{table_name}/2026/10/16/21"

            (
                df = spark.read.parquet(s3_path)
                    .withColumn("value", col("value").cast("string"))
                    .withColumn("data", from_json(col("value")))
            )
            dataframes[table.get("name")] = df["data"]
            continue


        #s3_path = get_s3_path(table).format(table_name=table_name)
    
        df = spark.read.parquet(s3_path)

        dataframes[table.get("name")] = df
    

    for name, df in dataframes.items():
        print(f"{name}:\n")
        df.show(2)
        print("\n\n")

    return dataframes