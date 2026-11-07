import os
from dotenv import load_dotenv

load_dotenv()

POSTGRE_HOST = os.getenv("POSTGRE_HOST")
POSTGRE_PORT = os.getenv("POSTGRE_PORT")
POSTGRE_DWH_DATABASE = os.getenv("POSTGRE_DWH_DATABASE")
POSTGRE_DWH_USER = os.getenv("POSTGRE_DWH_USER")
POSTGRE_DWH_PASSWORD = os.getenv("POSTGRE_DWH_PASSWORD")


POSTGRE_DWH_DDS_USER = os.getenv("POSTGRE_DWH_DDS_USER")
POSTGRE_DWH_DDS_PASSWORD = os.getenv("POSTGRE_DWH_DDS_PASSWORD")


def get_jdbc_options():
    
    return {
        "url": f"jdbc:postgresql://{POSTGRE_HOST}:{POSTGRE_PORT}/{POSTGRE_DWH_DATABASE}",
        "driver": "org.postgresql.Driver",
        "user": POSTGRE_DWH_USER,
        "password": POSTGRE_DWH_PASSWORD,
    }

def get_jdbc_POSTGRE_dds_options():
    
    return {
        "url": f"jdbc:postgresql://{POSTGRE_HOST}:{POSTGRE_PORT}/{POSTGRE_DWH_DATABASE}",
        "driver": "org.postgresql.Driver",
        "user": POSTGRE_DWH_DDS_USER,
        "password": POSTGRE_DWH_DDS_PASSWORD,
    }