from ..blob_storage_processor import BlobStorageProcessor
from .azure_storage_client import AzureStorageClient
from ..blob_storage_structure import BlobStorageStructure


class AzureBlobStorageProcessor(BlobStorageProcessor):  
    
    def __init__(self, blob_storage_structure:BlobStorageStructure,azure_storage_client:AzureStorageClient=None):
        if azure_storage_client is None:
            azure_storage_client = AzureStorageClient.get_instance()
        super().__init__(blob_storage_structure=blob_storage_structure, blob_storage_client=azure_storage_client)

    


