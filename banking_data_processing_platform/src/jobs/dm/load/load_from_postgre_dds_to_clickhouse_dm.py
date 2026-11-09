# coalesce(2)	    Сокращает число партиций без полного Shuffle
# numPartitions=2	Ограничивает параллельные JDBC-соединения
# batchsize=1000	Записывает данные пакетами по 1000 строк

def get_load_from_postgre_dds_to_clickhouse_dm(transformed_dataframes: dict, CLICKHOUSE_JDBC_OPTIONS: dict):

    for table_name, df in transformed_dataframes.items():

        df_load = df.coalesce(1)

        ( df_load.write
            .format("jdbc")
            .mode("append")
            .options(**CLICKHOUSE_JDBC_OPTIONS)
            .option("dbtable", table_name)
            .option("numPartitions", "1")
            .option("batchsize", "500")
            .save()
        )