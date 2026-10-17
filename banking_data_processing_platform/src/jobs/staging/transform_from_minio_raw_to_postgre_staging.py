import sys
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, to_timestamp
from pyspark.sql.types import FloatType, DoubleType

sys.path.append(str(Path(__file__).parent.parent.parent))
sys.path.append(str(Path(__file__).parent.parent.parent / "core"))

from metadata import get_all_tables
from type_mapping import parse_type


def get_transform_from_minio_raw_to_postgre_staging(dataframes: dict, config: dict):
    dataframes_transformed = {}
    tables = get_all_tables(config)
    dtypes = {table.get("name"):table.get("dtype") for table in tables}
    not_nulls = {table.get("name"):table.get("not_null", []) for table in tables} or {}

    for table_name, df in dataframes.items():

        if table_name not in dtypes:
            continue

        dtype = dtypes[table_name]
        not_null_set = set(not_nulls[table_name])

        validation_schema(df, table_name, not_null_set)
        df_transformed = transform_types(dtype, df)
        validate_not_null(not_null_set, df_transformed, table_name)

        dataframes_transformed[table_name] = df_transformed
        
    return dataframes_transformed



def validation_schema(df: DataFrame, table_name: str, not_null_set: set):
    current_columns = set(df.columns)

    missing_columns = not_null_set - current_columns
    if missing_columns:
        raise ValueError(f"Нет ожидаемых столбцов в {table_name}: {missing_columns}")



def transform_types(dtype: dict, df: DataFrame):
    df_transformed = df.select(*dtype)

    for column_name, column_type in dtype.items():

        if column_name not in df_transformed.columns:
            continue

        column_type_lower = column_type.lower().strip()

        if column_type_lower == 'datetype':
            expr = to_date(col(column_name), format="dd.MM.yyyy")
        elif column_type_lower == 'timestamptype':
            expr = to_timestamp(col(column_name), format="dd.MM.yyyy HH:mm")
        else:
            target_type = parse_type(column_type_lower)
            expr = col(column_name).cast(target_type)
            
        df_transformed = df_transformed.withColumn(column_name, expr)

    df_transformed.cache()

    return df_transformed



def validate_not_null(not_null_set: set, df_transformed: DataFrame, table_name: str):
    invalid_condition = None

    schema_field = {
        field.name:fiels.dataType
        for field in df_transformed.schema.fields
    }

    for column_name in not_null_set:

        condition = col(column_name).isNull() 
        
        if isinstance(schema_field[column_name], (FloatType, DoubleType)):
            condition = condition | col(column_name).isNaN()

        invalid_condition = (
            condition if invalid_condition is None
            else invalid_condition | condition)

    has_invalid = (
        df_transformed.filter(invalid_condition).limit(1).count()
    )

    if has_invalid:
        raise ValueError(
            f"Недопустимое NULL или NaN значение "
            f"в обязательных колонках таблицы {table_name}"
        )




