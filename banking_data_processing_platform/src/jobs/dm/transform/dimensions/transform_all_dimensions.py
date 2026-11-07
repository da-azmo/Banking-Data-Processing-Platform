import sys
from pathlib import Path
from pyspark.sql import DataFrame

sys.path.append(str(Path(__file__).parent.parent.parent.parent / "core"))
sys.path.append(str(Path(__file__).parent.parent / "common"))

from metadata import get_cur_dimension_config, get_all_dimension_or_fact_names_from_dm, get_all_dimensions_from_dm
from common_transform import rename_and_join_dim_or_fact

def get_transform_all_dimensions(config: dict, dataframes: dict, df_dim_dates: DataFrame):
    dim_transformed_dataframes = {}
    
    dim_names = get_all_dimension_or_fact_names_from_dm(get_all_dimensions_from_dm(config))

    for dim_name in dim_names:

        this_dim_config = get_cur_dimension_config(config, dim_name)
        if this_dim_config:
            result_df = rename_and_join_dim_or_fact(this_dim_config, dataframes, dim_name, df_dim_dates)

            dim_transformed_dataframes[dim_name] = result_df

    # for name, df in dim_transformed_dataframes.items():
    #     print(name)
    #     df.show()
    #     print()
    #     print()

    return dim_transformed_dataframes

