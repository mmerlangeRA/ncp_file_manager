from abc import ABC
from .blob_storage_structure import BlobStorageStructure, ContainerBase
from .blob_storage_client import BlobStorageClient

class BlobStorageProcessor(ABC):
    blob_storage_structure:BlobStorageStructure
    blob_storage_client: BlobStorageClient
    
    def __init__(self, blob_storage_structure:BlobStorageStructure,blob_storage_client:BlobStorageClient=None):
        self.blob_storage_structure = blob_storage_structure
        self.blob_storage_client=blob_storage_client
        
    def set_permissions(self, raw_permission="r", extracted_permission="w", processed_permission="w"):
        containers:list[ContainerBase] = [
            self.blob_storage_structure.container_raw, 
            self.blob_storage_structure.container_extracted, 
            self.blob_storage_structure.container_processed]
        
        permissions = [raw_permission,extracted_permission,processed_permission]
        for container,p in zip(containers,permissions): 
            if p == "w":
                sas_url = self.blob_storage_client.generate_url_with_permissions(container_name=container.name,read=True,list = True,write=True, create=True, delete=True)
            else:
                sas_url = self.blob_storage_client.generate_url_with_permissions(container_name=container.name,read=True,list = True,write=False, create=False, delete=False)
            container.sas_url =sas_url

