from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv

load_dotenv()

connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
container_name = "upload-bucket"

blob_service_client = BlobServiceClient.from_connection_string(connection_string)

def upload_file_to_blob(file_name, file_data):
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
        blob_client.upload_blob(file_data, overwrite=True)
        return blob_client.url
    except Exception as e:
        print(f"Error uploading file: {e}")
        return None
