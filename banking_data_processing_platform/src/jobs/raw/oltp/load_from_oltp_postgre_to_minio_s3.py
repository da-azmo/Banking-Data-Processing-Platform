import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent.parent / 'metadata'))

from metadata import get_s3_path

def get_load_from_oltp_postgre_to_minio_s3(dataframes: dict, config: dict):
    queries = []
    s3_path_without_table_name, s3_path_checkpoints_without_table_name = get_s3_path(config)

    for table_name, df in dataframes.items():
        path_table_name = table_name.split('.')[-1]

        s3_path_with_table_name = s3_path_without_table_name.format(table_name=path_table_name)
        s3_path_checkpoints_with_table_name = s3_path_checkpoints_without_table_name.format(table_name=path_table_name)

        query = (
            df.writeStream
                .format("parquet")
                .option("path", s3_path_with_table_name)
                .option("checkpointLocation", s3_path_checkpoints_with_table_name)
                .start()
        )

        queries.append(query)

    return queries