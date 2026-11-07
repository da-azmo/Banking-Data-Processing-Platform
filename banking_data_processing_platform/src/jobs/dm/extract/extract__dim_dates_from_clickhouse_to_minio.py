from pyspark.sql import SparkSession

def get_extract__dim_dates_from_clickhouse_to_minio(spark: SparkSession, CLICKHOUSE_OPTIONS: dict):
    
    df = (
        spark.read
        .format("jdbc")
        .options(**CLICKHOUSE_OPTIONS)
        .option('dbtable', 'dim_dates')
        .load()
    )

    return df