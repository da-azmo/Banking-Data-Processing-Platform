from pyspark.sql import DataFrame

def get_load_from_oltp_postgre_to_minio_s3(df: DataFrame):
    query = (
        df.writeStream
            .format("parquet")
            .option("path", "s3a://banking-data-processing-platform/raw/postgre/sources/clients/2026/10/08")
            .option("checkpointLocation", "s3a://banking-data-processing-platform/checkpoints/clients/2026/10/08")
            .start()
    )

    return query