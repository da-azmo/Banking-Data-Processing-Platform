import os
from dotenv import load_dotenv

load_dotenv()

def get_s3_connection_options():
    
    return {
        'access_key': os.getenv('MINIO_S3_ACCESS_KEY'),
        'secret_key': os.getenv('MINIO_S3_SECRET_KEY'),
        'endpoint': os.getenv('MINIO_S3_ENDPOINT'),
    }