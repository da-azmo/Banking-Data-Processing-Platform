
def get_load_from_minio_raw_to_postgre_staging(dataframes: dict, JDBC_OPTIONS: dict):

    for table_name, df in dataframes.items():

        (
            df.write
            .format("jdbc")
            .mode("overwrite")
            .options(**JDBC_OPTIONS)
            .option("dbtable", f"staging.{table_name}")
            .save()
        )