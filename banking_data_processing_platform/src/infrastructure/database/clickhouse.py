import os
from dotenv import load_dotenv

load_dotenv()

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")
CLICKHOUSE_DWH_DATABASE = os.getenv("CLICKHOUSE_DB")
CLICKHOUSE_DWH_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_DWH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")


def get_jdbc_CLICKHOUSE_options():
    
    return {
        "url": f"jdbc:clickhouse:http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DWH_DATABASE}",
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "user": CLICKHOUSE_DWH_USER,
        "password": CLICKHOUSE_DWH_PASSWORD,
    }