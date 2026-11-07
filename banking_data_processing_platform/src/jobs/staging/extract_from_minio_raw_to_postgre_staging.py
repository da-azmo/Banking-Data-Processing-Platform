from pyspark.sql import SparkSession
import sys
import json
from pathlib import Path
from pyspark.sql.functions import col, from_json, get_json_object
from pyspark.sql.types import StructType, StructField

sys.path.append(str(Path(__file__).parent.parent.parent / "core"))
sys.path.append(str(Path(__file__).parent.parent.parent))

from metadata import get_all_tables, get_s3_path
from type_mapping import parse_type

def get_extract_from_minio_raw_to_postgre_staging(spark: SparkSession, config: dict):

    dataframes = {}
    tables = get_all_tables(config)

    for table in tables:

        table_name = table.get("name")
        table_source = table.get("source")
        s3_path = get_s3_path(table).format(table_name=table_name)
        
        if table_source == "oltp":

            df = spark.read.parquet(s3_path).withColumn("value_json", col("value").cast("string"))

            # Row(value_json='{"schema": {...}, "payload": {...}}')
            row = df.select("value_json").first()

            # Превращаем JSON string в Python dict
            debezium_message = json.loads(row["value_json"])

            debezium_schema = debezium_message["schema"]["fields"]

            for part in debezium_schema:
                if part["field"] == "after":
                    debezium_schema_after = part["fields"]
                    break

            schema_arr = []
            for part in debezium_schema_after:
                schema_arr.append(StructField(part["field"], parse_type(part["type"]), part["optional"]))
            
            schema = StructType(schema_arr)

            df = df.withColumn("after_json", get_json_object(col("value_json"), "$.payload.after")).withColumn("data", from_json(col("after_json"), schema))

            dataframes[table.get("name")] = df.select("data.*")

            continue
        
        df = spark.read.parquet(s3_path)
        dataframes[table.get("name")] = df

    # for name, df in dataframes.items():
    #     print(name)
    #     df.show()
    #     print()
    #     print()

    return dataframes