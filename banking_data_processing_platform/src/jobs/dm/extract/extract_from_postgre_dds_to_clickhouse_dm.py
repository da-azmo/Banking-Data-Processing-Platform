from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pathlib import Path
import sys
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).parent.parent.parent / "core"))

from metadata import get_all_tables, get_all_dimensions_from_dm, get_all_facts_from_dm, get_all_dimension_or_fact_names_from_dm


def get_extract_from_postgre_dds_to_clickhouse_dm(spark: SparkSession, config_dds: dict, config_dm: dict, POSTGRE_JDBC_OPTIONS: dict, CLICKHOUSE_JDBC_OPTIONS: dict):
    dataframes = {}
    tables_dds = get_all_tables(config_dds)
    # get_max_dwh_loaded_at_from_clickhouse(spark, config_dm, CLICKHOUSE_JDBC_OPTIONS)
    max_dm_loaded_at = get_max_dm_loaded_at_from_clickhouse(spark, config_dm, CLICKHOUSE_JDBC_OPTIONS)

    for table in tables_dds:
        table_name = table.get("name")

        df_by_dwh_loaded_at = (
            spark.read
            .format("jdbc")
            .options(**POSTGRE_JDBC_OPTIONS)
            .option("query", f"SELECT * FROM dds.{table_name} WHERE dwh_loaded_at >= '{max_dm_loaded_at}'")
            .load()
        )

        if table_name == "cards_scd2":
            dataframes[f"{table_name}_sender"] = df_by_dwh_loaded_at
            dataframes[f"{table_name}_recipient"] = df_by_dwh_loaded_at
            continue

        dataframes[table_name] = df_by_dwh_loaded_at

    # for name, df in dataframes.items():
    #     print(name)
    #     df.show()
    #     print()
    
    return dataframes

def get_max_dm_loaded_at_from_clickhouse(spark: SparkSession, config_dm: dict, CLICKHOUSE_JDBC_OPTIONS: dict):
    max_dm_loaded_at = []
    tables_dm = get_all_dimension_or_fact_names_from_dm(get_all_dimensions_from_dm(config_dm)) | get_all_dimension_or_fact_names_from_dm(get_all_facts_from_dm(config_dm))

    for table in tables_dm:
        try:

            df_by_dm_loaded_at = (
                spark.read
                .format("jdbc")
                .options(**CLICKHOUSE_JDBC_OPTIONS)
                .option("query", f"SELECT COALESCE(MAX(dm_loaded_at), toDateTime('1990-01-01 00:00:00')) as max_dm_loaded_at FROM banking_data_processing_platform_dm.{table}")
                .load()
            )
            
            max_dm_loaded_at.append(df_by_dm_loaded_at.first()["max_dm_loaded_at"])

        except Exception:
            raise ValueError(f"Таблицы с именем {table} в ClickHouse DM не существует.")

    # for name, df in dict_max_dwh_loaded_at.items():
    #     print(name)
    #     print(df)
    #     print()
    #     print()
    
    return max(max_dm_loaded_at)

