import sys
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_from_unix_date, timestamp_micros, to_date, to_timestamp
from pyspark.sql.types import FloatType, DoubleType

sys.path.append(str(Path(__file__).parent.parent.parent))
sys.path.append(str(Path(__file__).parent.parent.parent / "core"))

from metadata import get_all_tables
from type_mapping import parse_type


def get_transform_from_minio_raw_to_postgre_staging(dataframes: dict, config: dict):
    valid_dataframes_transformed = {}
    invalid_dataframes_transformed = {}
    tables = get_all_tables(config)
    dtypes = {table.get("name"):table.get("dtype") for table in tables}
    not_nulls = {table.get("name"):table.get("not_null", []) for table in tables} or {}
    table_rename = {table.get("name"):table.get("columns", []) for table in tables} or {}
    sources = {table.get("name"):table.get("source") for table in tables} or {}

    for table_name, df in dataframes.items():

        if table_name not in dtypes:
            continue

        dtype = dtypes[table_name]
        not_null_set = set(not_nulls[table_name])
        source = sources[table_name]

        df_renamed = rename_columns(df, table_name, table_rename)
        missing_columns = validation_schema(df_renamed, table_name, not_null_set)

        if missing_columns:
            invalid_dataframes_transformed[table_name] = df_renamed
            continue

        df_with_Null_NaN = transform_Null_NaN(df_renamed)
        df_transformed = transform_types(dtype, df_with_Null_NaN, source)
        valid_df, invalid_df = validate_not_null(not_null_set, df_transformed, table_name)

        valid_dataframes_transformed[table_name] = valid_df
        invalid_dataframes_transformed[table_name] = invalid_df

    # for name, df_t in valid_dataframes_transformed.items():
    #     print(name)
    #     df_t.show()
    #     print()
    #     print()

    return valid_dataframes_transformed, invalid_dataframes_transformed



def rename_columns(df: DataFrame, table_name: str, table_rename: dict):
    this_table_rename_columns = table_rename[table_name]

    df = df.select(
        *[col(source).alias(target)
        for source, target in this_table_rename_columns.items()])

    return df



def validation_schema(df: DataFrame, table_name: str, not_null_set: set):
    current_columns = set(df.columns)

    missing_columns = not_null_set - current_columns
    
    return missing_columns



def transform_Null_NaN(df: DataFrame):
    df = df.replace(["NULL", "NaN"], None)

    return df



def transform_types(dtype: dict, df: DataFrame, source: str):
    df_transformed = df.select(*dtype)

    for column_name, column_type in dtype.items():

        if column_name not in df_transformed.columns:
            continue

        column_type_lower = column_type.lower().strip()

        if column_type_lower == 'datetype':
            if source == "oltp":
                expr = date_from_unix_date(col(column_name))
            else:
                expr = to_date(col(column_name), format="yyyy-MM-dd")
        elif column_type_lower == 'timestamptype':
            if source == "oltp":
                expr = timestamp_micros(col(column_name))
            elif source == "json":
                expr = to_timestamp(col(column_name), format="yyyy-MM-dd'T'HH:mm:ss")
            else:
                expr = to_timestamp(col(column_name), format="dd.MM.yyyy H:mm")
        else:
            target_type = parse_type(column_type_lower)
            expr = col(column_name).cast(target_type)
            
        df_transformed = df_transformed.withColumn(column_name, expr)

    df_transformed.cache()

    return df_transformed



def validate_not_null(not_null_set: set, df_transformed: DataFrame, table_name: str):
    invalid_condition = None

    schema_field = {
        field.name:field.dataType
        for field in df_transformed.schema.fields
    }

    for column_name in not_null_set:

        condition = col(column_name).isNull()
        
        # if isinstance(schema_field[column_name], (FloatType, DoubleType)):
        #     condition = condition | col(column_name).isNaN()

        invalid_condition = (
            condition if invalid_condition is None
            else invalid_condition | condition)

    valid_df = df_transformed.filter(~invalid_condition)
    invalid_df = df_transformed.filter(invalid_condition)

    return valid_df, invalid_df

    # has_invalid = (
    #     df_transformed.filter(invalid_condition).limit(1).count()
    # )

    # if has_invalid:
    #     raise ValueError(
    #         f"Недопустимое NULL или NaN значение "
    #         f"в обязательных колонках таблицы {table_name}"
    #     )




