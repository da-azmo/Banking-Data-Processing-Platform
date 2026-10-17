import json
import yaml
from pathlib import Path

CONFIGS = str(Path(__file__).parent.parent / 'configs')

def get_config(config_name: str, type_source: str):

    with open(f"{CONFIGS}/{config_name}.yml", "r", encoding="UTF-8") as file:
        config_file = yaml.safe_load(file)

    return config_file.get(type_source)


def get_debezium_connector_config(config_name: str):

    with open(f"{CONFIGS}/{config_name}.json", "r", encoding="UTF-8") as file:
        connector_config = json.load(file)

    return connector_config