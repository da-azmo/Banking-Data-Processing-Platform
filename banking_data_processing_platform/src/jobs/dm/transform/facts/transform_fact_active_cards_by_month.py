import sys
from pathlib import Path
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

sys.path.append(str(Path(__file__).parent.parent.parent.parent / "core"))
sys.path.append(str(Path(__file__).parent.parent / "common"))

from common_transform import rename_and_join_dim_or_fact
from metadata import get_cur_fact_config

THIS_FACT_NAME = "fact_active_cards_by_month"

def get_transform_fact_active_cards_by_month(config: dict, dataframes: dict, df_dim_dates: DataFrame):
    this_fact_config = get_cur_fact_config(config, THIS_FACT_NAME)
    
    result_df = rename_and_join_dim_or_fact(this_fact_config, dataframes, THIS_FACT_NAME, df_dim_dates, "valid_from", "valid_to")

    result_df = ( result_df
        .groupby("year", "month_name")
        .agg(
            F.count("card_bk_sender").alias("active_cards_count"))
        .withColumn('dm_loaded_at', F.current_timestamp())
    )

    # for name, df in fact_transformed_dataframes.items():
    #     print(name)
    #     df.show()
    #     print()
    #     print()

    return result_df