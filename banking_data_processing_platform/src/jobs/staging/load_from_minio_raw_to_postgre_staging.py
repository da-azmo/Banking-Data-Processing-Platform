
def get_load_from_minio_raw_to_postgre_staging(valid_transformed_dataframes: dict, invalid_transformed_dataframes: dict, JDBC_OPTIONS: dict):

    for table_name, df in valid_transformed_dataframes.items():

        (
            df.write
            .format("jdbc")
            .mode("overwrite")
            .options(**JDBC_OPTIONS)
            .option("dbtable", f"staging.{table_name}")
            .save()
        )

    for table_name, df in invalid_transformed_dataframes.items():

        (
            df.write
            .format("jdbc")
            .mode("overwrite")
            .options(**JDBC_OPTIONS)
            .option("dbtable", f"staging.invalid_{table_name}")
            .save()
        )