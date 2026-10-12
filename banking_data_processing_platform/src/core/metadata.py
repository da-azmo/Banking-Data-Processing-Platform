import sys
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path(__file__).parent))


# получить таблицы
def get_all_tables(config: dict):

    return config.get('tables')



# получить s3-path
def get_s3_path(config: dict):

    current_datetime = datetime.now()

    target = config.get('s3_path').format(
            table_name="{table_name}",
            year=current_datetime.year,
            month=current_datetime.month,
            day=current_datetime.day,
            hour=current_datetime.hour)

    if "s3_checkpoint" in config.keys():
        target_checkpoint = config.get('s3_checkpoint').format(
            table_name="{table_name}")

        return target, target_checkpoint

    return target



def get_debezium_oltp_tables_names(config: dict):
    tables_name = []
    prefix = config.get("config").get("topic.prefix")
    tables = config.get("config").get("table.include.list").split(",")

    for table in tables:
        tables_name.append(f"{prefix}.{table}")
    
    return tables_name