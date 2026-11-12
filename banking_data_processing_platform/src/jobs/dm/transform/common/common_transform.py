from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent.parent / "core"))

from common_validation import validation_sources_from_dds, validation_columns_from_dds
from metadata import get_all_dm_dds_columns, get_cur_source_sk_key

def rename_and_join_dim_or_fact(config: dict, dataframes: dict, dim_or_fact_name: str, df_dim_dates: DataFrame, DATE_OPTION="", DATE_OPTION_2=""):
    result_df = None

    for source_name, sk_and_columns in config.items():
        cur_sk_key = get_cur_source_sk_key(sk_and_columns)
        columns = get_all_dm_dds_columns(sk_and_columns)

        if source_name != "dim_dates":

            validation_sources_from_dds(source_name, dim_or_fact_name, dataframes)
            
            cur_dds_df = dataframes.get(source_name)
            
            validation_columns_from_dds(columns, dim_or_fact_name, cur_dds_df)

            cur_dm_df = rename_columns_dim(cur_dds_df, columns)

            if result_df is None:
                result_df = cur_dm_df
            else:
                result_df = result_df.join(cur_dm_df, on=cur_sk_key, how="left")
                
        else:
            columns_dim_date = list(columns.keys()) or []
            result_df = transform_and_rename_date_option(result_df, DATE_OPTION, cur_sk_key)
            
            if DATE_OPTION_2:
                try:
                    result_df = get_valid_df_for_DATE_OPTION_2(result_df, DATE_OPTION_2)
                except Exception:
                    raise ValueError(f"В таблице dds {source_name} проблема с полем {DATE_OPTION_2}")
            
            cur_df_dim_dates = df_dim_dates[columns_dim_date]

            result_df = result_df.join(F.broadcast(cur_df_dim_dates), on=cur_sk_key, how="left")

    return result_df



def rename_columns_dim(cur_dds_df: DataFrame, columns: dict):

    cur_dm_df = cur_dds_df.select(*[
            F.col(col_dds).alias(col_dm)
            for col_dm, col_dds in columns.items()
        ])

    return cur_dm_df



def transform_and_rename_date_option(result_df: DataFrame, DATE_OPTION: str, cur_sk_key: str):

    df_with_transformed_DATE_OPTION = ( result_df
        .withColumn(f"{cur_sk_key}_DateType", F.to_date(F.col(DATE_OPTION), 'yyyy-MM-dd'))
        .withColumn(cur_sk_key, F.date_format(F.col(f"{cur_sk_key}_DateType"), 'yyyyMMdd').cast("bigint"))
    )

    return df_with_transformed_DATE_OPTION



def get_valid_df_for_DATE_OPTION_2(result_df: DataFrame, DATE_OPTION_2: str):

    valid_conditions = ( F.col(DATE_OPTION_2) >= F.current_timestamp())

    result_df = result_df.filter(valid_conditions)

    return result_df
