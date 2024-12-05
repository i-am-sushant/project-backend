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

def download_file_from_blob(file_name):
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
        download_stream = blob_client.download_blob()
        return download_stream.readall()
    except Exception as e:
        print(e)
        return None
    
def delete_file_from_blob(file_name):
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
        blob_client.delete_blob()
        return True
    except Exception as e:
        print(e)
        return False
    
def list_blobs_in_container():
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blob_list = container_client.list_blobs()
        files = []
        for blob in blob_list:
            blob_client = container_client.get_blob_client(blob.name)
            properties = blob_client.get_blob_properties()
            files.append({
                'file_name': blob.name,
                'file_size': properties.size / 1024,  # Size in KB
                'content_type': properties.content_settings.content_type,
                'blob_url': blob_client.url
            })
        return files
    except Exception as e:
        print(f"Error listing blobs: {e}")
        return []