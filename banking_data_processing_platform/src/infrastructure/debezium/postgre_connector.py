import os
from dotenv import load_dotenv
import requests

load_dotenv()

CONNECT_URL = os.getenv("CONNECT_URL")

def check_debezium_connector(debezium_connector_config: dict):

    CONNECT_NAME = debezium_connector_config.get("name")

    response = requests.get(
        f"{CONNECT_URL}/connectors/{CONNECT_NAME}",
        timeout=30)

    if response.status_code == 200:
        print(f"Connector {CONNECT_NAME} already exists")
    
    elif response.status_code == 404:
        response = requests.post(
            f"{CONNECT_URL}/connectors",
            json=debezium_connector_config,
            timeout=30)
        if response.status_code >= 400:
            print("STATUS:", response.status_code)
            print("RESPONSE:", response.text)

        #response.raise_for_status()

        print(f"Connector {CONNECT_NAME} has been created")

    else:
        response.raise_for_status()
