import sys
from pathlib import Path
from pyspark.sql import SparkSession

sys.path.append(str(Path(__file__).parent / "dimensions"))
sys.path.append(str(Path(__file__).parent / "facts"))
sys.path.append(str(Path(__file__).parent.parent.parent / "jobs" / "dm" / "extract"))
sys.path.append(str(Path(__file__).parent.parent.parent / "core"))

from metadata import get_all_dimensions_from_dm, get_all_facts_from_dm
from extract___dim_dates_from_minio import get_extract___dim_dates_from_minio
from transform_all_dimensions import get_transform_all_dimensions
from transform_fact_payments import get_transform_fact_payments
from transform_fact_transactions import get_transform_fact_transactions
from transform_fact_transactions_by_day import get_transform_fact_transactions_by_day
from transform_fact_clients_with_one_transaction_at_least_by_day import get_transform_fact_clients_with_one_transaction_at_least_by_day
from transform_fact_transactions_exchange_rates_by_month import get_transform_fact_transactions_exchange_rates_by_month
from transform_fact_popularity_purchase_category import get_transform_fact_popularity_purchase_category
from transform_fact_transaction_by_client import get_transform_fact_transaction_by_client
from transform_fact_active_cards_by_month import get_transform_fact_active_cards_by_month

def get_transform_from_postgre_dds_to_clickhouse_dm(spark: SparkSession, dataframes: dict, config_dm: dict, config_dm_result_columns: dict):
    transformed_dataframes = {}
    dict_dimension_or_fact_columns = get_all_dimensions_from_dm(config_dm_result_columns) | get_all_facts_from_dm(config_dm_result_columns)

    df_dim_dates = get_extract___dim_dates_from_minio(spark, config_dm)

    transformed_dataframes = get_transform_all_dimensions(config_dm, dataframes, df_dim_dates)
    transformed_dataframes["fact_payments"] = get_transform_fact_payments(config_dm, dataframes, df_dim_dates)
    transformed_dataframes["fact_transactions"] = get_transform_fact_transactions(config_dm, dataframes, df_dim_dates)
    transformed_dataframes["fact_transactions_by_day"] = get_transform_fact_transactions_by_day(config_dm, dataframes, df_dim_dates)
    transformed_dataframes["fact_clients_with_one_transaction_at_least_by_day"] = get_transform_fact_clients_with_one_transaction_at_least_by_day(config_dm, dataframes, df_dim_dates)
    transformed_dataframes["fact_transactions_exchange_rates_by_month"] = get_transform_fact_transactions_exchange_rates_by_month(config_dm, dataframes, df_dim_dates)
    transformed_dataframes["fact_popularity_purchase_category"] = get_transform_fact_popularity_purchase_category(config_dm, dataframes, df_dim_dates)
    transformed_dataframes["fact_transaction_by_client"] = get_transform_fact_transaction_by_client(config_dm, dataframes, df_dim_dates)
    transformed_dataframes["fact_active_cards_by_month"] = get_transform_fact_active_cards_by_month(config_dm, dataframes, df_dim_dates)

    for table_name, df in transformed_dataframes.items():
        try:
            list_this_table_columns = dict_dimension_or_fact_columns.get(table_name, [])
            transformed_dataframes[table_name] = df[list_this_table_columns]
        except Exception:
            raise ValueError(f"В таблице {table_name} отсутствуют необходимые столбцы (один или более)")
    
    # for name, df in transformed_dataframes.items():
    #     print(name)
    #     df.show(5)
    #     print()
    #     print()

    return transformed_dataframes








