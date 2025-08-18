from abc import ABC, abstractmethod
from typing import Self
from .settings import *

class BlobStorageClient(ABC):
    """
    This class is responsible for all atomic operations on the BlobStorage
    """
    
    @classmethod
    def get_instance(cls)->Self:
        pass
    
    @abstractmethod    
    def create_record_container_pseudo_folder(self, container_name:str,record_path:str):
       pass
    
    @abstractmethod
    def create_calibrations_container_pseudo_folder(self, container_name:str,calibrations_path:str):
        """Create pseudo folder for calibrations"""
        pass

    @abstractmethod
    def generate_sas_token(self, container_name:str,read=False,add=False,list = False,write=False, create=False, delete=False,hours=12):
        pass
    
    @abstractmethod
    def generate_blob_upload_sas_token(self,container_name:str, blob_name:str, hours=12)->str:
        pass

    @abstractmethod
    def generate_blob_download_sas_token(self,container_name:str, blob_name:str, hours=24)->str:
        pass
    
    @abstractmethod
    def generate_blob_upload_url(self, container_name:str,blob_name:str, hours=12)->str:
        pass
    
    @abstractmethod
    def generate_url_with_permissions(self, container_name:str,read=False,add=False,list = False,write=False, create=False, delete=False,hours=12):
        pass

    @abstractmethod
    def generate_container_read_write_url(self,container_name:str,hours=24)->str:
        pass

    @abstractmethod
    def generate_container_read_url(self, container_name:str,hours=24)->str:
        pass
    
    @abstractmethod
    def generate_blob_download_url(self, container_name:str,blob_name:str)->str:
        pass
    
    @abstractmethod
    def check_blob_exists(self, container_name:str,blob_name:str)->bool:
        pass

    @abstractmethod
    def get_blob_size(self, container_name:str,blob_name:str)->int:
        pass
    
    @abstractmethod
    def list_blob_download_urls(self, container_name:str, prefix:str)->list[str]:
        """
        List all blobs starting with the given prefix and return their download URLs.
        
        Args:
            container_name (str): The name of the container
            prefix (str): The prefix to filter blobs by
            
        Returns:
            list[str]: A list of download URLs for all blobs matching the prefix
        """
        pass
    
    @abstractmethod
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
        pass
    
    @abstractmethod
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
        pass

    @abstractmethod
    def change_blob_access_tier(self, container_name: str, blob_name: str, access_tier: str) -> None:
        """
        Change the access tier of a blob in the specified container.

        Args:
            container_name (str): The name of the container.
            blob_name (str): The name of the blob.
            access_tier (str): The new access tier to set for the blob.
        """
        pass
