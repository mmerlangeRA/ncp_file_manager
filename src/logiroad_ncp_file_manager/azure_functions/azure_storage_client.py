from datetime import datetime, timedelta, timezone
import logging
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions, generate_container_sas, ContainerSasPermissions, ContainerClient, StandardBlobTier

from ..blob_storage_client import BlobStorageClient
from ..settings import *

logger = logging.getLogger(__name__)

class AzureStorageClient(BlobStorageClient):
    connection_string:str
    account_name:str
    account_key:str

    @classmethod
    def get_instance(cls):
        if AZURE_STORAGE_CONNECTION_STRING:   
            client = cls()
            return client
        else:
            return None
    
    def __init__(self):
        
        # Try to get connection string directly from environment first
        self.connection_string = AZURE_STORAGE_CONNECTION_STRING
        self.account_name = AZURE_ACCOUNT_NAME
        self.account_key = AZURE_ACCOUNT_KEY
        
        if not self.connection_string:
            # If still no connection string, try to build it from components
            if not all([self.account_name, self.account_key]):
                raise ValueError("Azure Storage credentials not properly configured. Check your environment variables.")
            
            self.connection_string = (
                f"DefaultEndpointsProtocol=https;"
                f"AccountName={self.account_name};"
                f"AccountKey={self.account_key};"
                f"EndpointSuffix=core.windows.net"
            )
        
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
    
    def get_container_client(self, container_name:str)->ContainerClient:
        return self.blob_service_client.get_container_client(container_name)

    def create_record_container_pseudo_folder(self, container_name:str,record_path:str):
        """Create pseudo folder for a record"""
        # Create a dummy blob to ensure the directory structure exists
        self.get_container_client(container_name).upload_blob(
            name=record_path + ".keep",
            data="",
            overwrite=True
        )
    
    def create_calibrations_container_pseudo_folder(self, container_name:str,calibrations_path:str):
        """Create pseudo folder for calibrations"""
        #check if blob exists
        blob_client = self.get_container_client(container_name).get_blob_client(calibrations_path)
        if blob_client.exists():
            logger.warning(f"Blob {calibrations_path} already exists.")
            return
        # Create a dummy blob to ensure the directory structure exists
        self.get_container_client(container_name).upload_blob(
            name=calibrations_path + ".keep",
            data="",
            overwrite=True
        )

    def generate_sas_token(self, container_name:str,read=False,add=False,list = False,write=False, create=False, delete=False,hours=12):
        sas_token = generate_container_sas(
            account_name=self.account_name,
            container_name=container_name,
            account_key=self.account_key,
            permission=ContainerSasPermissions(write=write, create=create,add=add, list=list,delete=delete, read=read),
            expiry=datetime.now(timezone.utc) + timedelta(hours=hours)
        )
        return sas_token
    
    def generate_blob_upload_sas_token(self,container_name:str, blob_name:str, hours=12)->str:
        sas_token = generate_blob_sas(
            account_name=self.account_name,
            container_name=container_name,
            blob_name=blob_name,
            account_key=self.account_key,
            permission=BlobSasPermissions(write=True, create=True),
            expiry=datetime.now(timezone.utc) + timedelta(hours=hours)
        )
        return sas_token

    def generate_blob_download_sas_token(self,container_name:str, blob_name:str, hours=24)->str:
        sas_token = generate_blob_sas(
            account_name=self.account_name,
            container_name=container_name,
            blob_name=blob_name,
            account_key=self.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc) + timedelta(hours=hours)
        )
        return sas_token

    def generate_blob_upload_url(self, container_name:str,blob_name:str, hours=12)->str:
        sas_token = self.generate_blob_upload_sas_token(container_name=container_name,blob_name=blob_name, hours=hours)
        return f"https://{self.account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
        
    def generate_url_with_permissions(self, container_name:str,read=False,add=False,list = False,write=False, create=False, delete=False,hours=12):
        sas_token = self.generate_sas_token(container_name,read=read,add=add,list=list,write=write,create=create,delete=delete,hours=hours)
        return f"https://{self.account_name}.blob.core.windows.net/{container_name}?{sas_token}"

    def generate_container_read_write_url(self,container_name:str,hours=24)->str:
        return self.generate_url_with_permissions(container_name=container_name, read=True, add=True, list=True, write=True, create=True, delete=True, hours=hours)

    def generate_container_read_url(self, container_name:str,hours=24)->str:
        return self.generate_url_with_permissions(container_name=container_name, read=True, list=True, write=False, create=False, delete=False,hours=hours)
    
    def generate_blob_download_url(self, container_name:str,blob_name:str)->str:
        sas_token = self.generate_blob_download_sas_token(container_name=container_name, blob_name= blob_name)
        return f"https://{self.account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"

    def check_blob_exists(self, container_name:str,blob_name:str)->bool:
        container_client = self.get_container_client(container_name=container_name)
        blob_client = container_client.get_blob_client(blob_name)
        return blob_client.exists()

    def get_blob_size(self, container_name:str,blob_name:str)->int:
        container_client = self.get_container_client(container_name=container_name)
        blob_client = container_client.get_blob_client(blob_name)
        properties = blob_client.get_blob_properties()
        return properties.size
        
    def list_blob_download_urls(self, container_name:str, prefix:str)->list[str]:
        """
        List all blobs starting with the given prefix and return their download URLs.
        
        Args:
            container_name (str): The name of the container
            prefix (str): The prefix to filter blobs by
            
        Returns:
            list[str]: A list of download URLs for all blobs matching the prefix
        """
        container_client = self.get_container_client(container_name=container_name)
        
        # List all blobs with the given prefix
        blob_list = container_client.list_blobs(name_starts_with=prefix)
        
        # Generate download URLs for each blob
        download_urls = []
        for blob in blob_list:
            # Skip directory markers or empty blobs
            if blob.name.endswith('.keep') or blob.size == 0:
                continue
                
            download_url = self.generate_blob_download_url(
                container_name=container_name,
                blob_name=blob.name
            )
            download_urls.append(download_url)
            
        return download_urls
        
    def list_blob_download_urls_with_folders(self, container_name:str, prefix:str)->dict[str, list[str]]:
        """
        List all blobs starting with the given prefix, grouped by pseudo folders, and return their download URLs.
        
        Args:
            container_name (str): The name of the container
            prefix (str): The prefix to filter blobs by
            
        Returns:
            dict[str, list[str]]: A dictionary where keys are folder paths and values are lists of download URLs.
                                 The special key "root" contains blobs directly in the prefix without any subfolder.
        """
        if not prefix.endswith("/"):
            prefix+="/"
        container_client = self.get_container_client(container_name=container_name)
        
        # List all blobs with the given prefix
        blob_list = container_client.list_blobs(name_starts_with=prefix)
        
        # Group blobs by folder
        folders = {}
        folders["root"] = []  # Special key for blobs directly in the prefix
        
        for blob in blob_list:
            # Skip directory markers or empty blobs
            if blob.name.endswith('.keep') or blob.size == 0:
                continue
                
            # Generate download URL
            download_url = self.generate_blob_download_url(
                container_name=container_name,
                blob_name=blob.name
            )
            
            # Extract the relative path from the prefix
            relative_path = blob.name[len(prefix):]
            
            # Check if the blob is in a subfolder
            if '/' in relative_path:
                # Extract the folder path
                folder_path = relative_path.split('/')[0]
                # Initialize the folder list if it doesn't exist
                if folder_path not in folders:
                    folders[folder_path] = []
                    
                # Add the download URL to the folder list
                folders[folder_path].append(download_url)
            else:
                # Add the download URL to the root list
                folders["root"].append(download_url)
        # Remove empty folders
        folders = {k: v for k, v in folders.items() if v}
            
        return folders

    def delete_blobs_by_prefix(self, container_name: str, prefix: str) -> int:
        """
        Deletes all blobs within a specified container that match a given prefix
        using a batch operation.

        Args:
            container_name (str): The name of the container.
            prefix (str): The prefix to filter blobs by. All blobs starting with this prefix will be deleted.
        
        Returns:
            int: The number of blobs submitted for deletion.
        """
        container_client = self.get_container_client(container_name=container_name)
        blob_list = container_client.list_blobs(name_starts_with=prefix)
        
        blobs_to_delete = [blob.name for blob in blob_list if not blob.name.endswith('/.keep')] # Ensure we don't delete our pseudo-folder markers
        
        if not blobs_to_delete:
            logger.debug(f"No blobs found with prefix '{prefix}' in container '{container_name}' to delete.")
            return 0

        logger.debug(f"Attempting to batch delete {len(blobs_to_delete)} blobs with prefix '{prefix}' in container '{container_name}'.")
        
        try:
            # The delete_blobs operation can take an iterable of blob names.
            # It's a more efficient way to delete multiple blobs.
            results = container_client.delete_blobs(*blobs_to_delete)
            # The `results` object is an iterator. Successfully queued deletions might not raise an error immediately.
            # For simplicity, we'll assume success if no immediate exception is raised by the call.
            # Proper error handling for partial failures in batch operations can be complex
            # and might involve checking each item in the `results` iterator if the SDK version provides such details.
            # For now, we return the count of blobs submitted.
            
            # Iterate through results to check for errors, if the SDK provides this.
            # Some versions/operations might raise AzureBatchOperationError for partial failures.
            # For azure-storage-blob, delete_blobs returns an iterator of responses (None for success, exception for failure).
            successful_deletions = 0
            errors_count = 0
            for result in results:
                if result is None: # Assuming None means success for that specific blob
                    successful_deletions +=1
                else: # An exception object indicates failure for that blob
                    errors_count +=1
                    logger.error(f"Failed to delete a blob: {result}")

            if errors_count > 0:
                logger.error(f"Batch deletion completed with {errors_count} errors out of {len(blobs_to_delete)} submitted.")
            else:
                logger.debug(f"Successfully submitted batch deletion request for {len(blobs_to_delete)} blobs.")
            
            return len(blobs_to_delete) # Return total submitted, or successful_deletions for more accuracy
        except Exception as e:
            # This would catch errors like authentication issues, container not found, or if the batch operation itself fails.
            logger.error(f"An error occurred during batch deletion for prefix '{prefix}' in container '{container_name}': {str(e)}")
            # Depending on the exception, it might indicate total failure or partial.
            # For a general exception here, assume all submitted blobs failed.
            return 0

    def change_blob_access_tier(self, container_name: str, blob_name: str, access_tier: StandardBlobTier) -> bool:
        """
        Change the access tier of a blob in the specified container.

        Args:
            container_name (str): The name of the container.
            blob_name (str): The name of the blob.
            access_tier (StandardBlobTier): The new access tier to set for the blob.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        try:
            blob_client = self.get_container_client(container_name).get_blob_client(blob_name)
            blob_client.set_standard_blob_tier(access_tier)
            logger.info(f"Changed access tier of blob '{blob_name}' in container '{container_name}' to '{access_tier}'.")
            return True
        except Exception as e:
            logger.error(f"Failed to change access tier for blob '{blob_name}' in container '{container_name}': {str(e)}")
            return False
