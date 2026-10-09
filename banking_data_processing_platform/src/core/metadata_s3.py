import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from config import get_config


# получить config s3
def get_s3_config():
    
    CONFIG = get_config('csv-source')

    return CONFIG.get('csv')

# получить s3-path
def get_s3_path(s3_config: dict):

    return s3_config.get('s3_path')

# получить таблицы s3
def get_s3_tables(s3_config: dict):

    return s3_config.get('tables')