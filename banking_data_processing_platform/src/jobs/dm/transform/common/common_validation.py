from pyspark.sql import DataFrame

def validation_sources_from_dds(source: str, DIM_OR_FACT_NAME: str, dataframes: dict):

    if source not in dataframes:
        raise ValueError(f"В извлеченных из DDS данных отсуствует необходимая для {DIM_OR_FACT_NAME} таблица {source}")


def validation_columns_from_dds(columns: dict, DIM_OR_FACT_NAME: str, cur_dds_df: DataFrame):
    
    dm_columns_set = set(columns.values())
    dds_columns_set = set(cur_dds_df.columns)

    missing_columns = dm_columns_set - dds_columns_set

    if missing_columns:
        raise ValueError(f"В извлеченных из DDS данных для {DIM_OR_FACT_NAME} отсуствуют столбцы: {missing_columns}")