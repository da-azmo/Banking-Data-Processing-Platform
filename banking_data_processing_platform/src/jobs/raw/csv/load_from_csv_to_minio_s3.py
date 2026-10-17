import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent.parent / "core"))

from metadata import get_s3_path

def get_load_from_csv_to_minio_s3(dataframes: dict, csv_config: dict):

    s3_path_without_table_name = get_s3_path(csv_config)

    for table_name, df in dataframes.items():

        s3_path_with_table_name = s3_path_without_table_name.format(table_name=table_name)

        (df.write
            .format("parquet") 
            .mode("append")
            .save(s3_path_with_table_name))

