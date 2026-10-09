import sys
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent.parent.parent / "core"))

from metadata_s3 import get_s3_path

def get_load_from_csv_postgre_to_minio_s3(dataframes: dict, config: dict):

    s3_path = get_s3_path(config)
    current_datetime = datetime.now()

    for table_name, df in dataframes.items():

        target = s3_path.format(
            table_name=table_name,
            year=current_datetime.year,
            month=current_datetime.month,
            day=current_datetime.day,
            hour=current_datetime.hour)

        (df.write
            .format("parquet") 
            .mode("append")
            .save(target))

