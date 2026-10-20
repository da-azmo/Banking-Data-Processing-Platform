from pyspark.sql.types import StringType, DateType, BooleanType, TimestampType, DateType, DecimalType, LongType, IntegerType

TYPE_MAPPING = {
    "stringtype": StringType(),
    "datetype": DateType(),
    "booleantype": BooleanType(),
    "timestamptype": TimestampType(),
    "datetype": DateType(),
    "decimaltype(10, 2)": DecimalType(10, 2),
    "decimaltype(20, 2)": DecimalType(20, 2),
    "string": StringType(),
    "int64": LongType(),
    "int32": IntegerType(),
}

def parse_type(type_column: str):

    if isinstance(type_column, str):
        base_type = type_column.lower().strip()
        return TYPE_MAPPING.get(base_type, StringType())

    return StringType()