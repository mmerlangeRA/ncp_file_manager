import os
from dotenv import load_dotenv
load_dotenv()
print("version = 0.1.1")
default_blob_storage_structure_template_path = "blob_storage_structure_template.json"

AZURE_ACCOUNT_NAME=os.getenv("AZURE_ACCOUNT_NAME")
AZURE_ACCOUNT_KEY=os.getenv("AZURE_ACCOUNT_KEY")
AZURE_STORAGE_CONNECTION_STRING=os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER_RAW=os.getenv("AZURE_CONTAINER_RAW")
AZURE_CONTAINER_EXTRACTED=os.getenv("AZURE_CONTAINER_EXTRACTED")
AZURE_CONTAINER_PROCESSED=os.getenv("AZURE_CONTAINER_PROCESSED")

AZURE_CONTAINERS={
    'raw': AZURE_CONTAINER_RAW,
    'extracted': AZURE_CONTAINER_EXTRACTED,
    'processed': AZURE_CONTAINER_PROCESSED
}