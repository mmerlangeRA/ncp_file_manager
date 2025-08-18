"""
LogiRoad NCP File Manager

A file manager for LogiRoad data on Azure Blob Storage.
"""

from .__version__ import __version__, __author__, __email__

# Core classes
from .ncp_file_manager_class import NCPFileManager
from .blob_storage_structure import (
    BlobStorageStructure,
    ContainerBase,
    ContainerRaw,
    ContainerExtracted,
    ContainerProcessed,
    L2R_ResultFileNameManager,
)
from .blob_storage_client import BlobStorageClient
from .blob_storage_processor import BlobStorageProcessor
from .models import Record, Camera, CalibrationVideo
from .const import ContainerTypeEnum, L2R_ResultFile, NCP_ResultFile
from .settings import *

# Azure functions
from .azure_functions import (
    AzureManager,
    AzureStorageClient,
    AzureBlobStorageProcessor,
)

__all__ = [
    # Version info
    "__version__",
    "__author__",
    "__email__",
    
    # Core classes
    "NCPFileManager",
    "BlobStorageStructure",
    "ContainerBase",
    "ContainerRaw", 
    "ContainerExtracted",
    "ContainerProcessed",
    "L2R_ResultFileNameManager",
    "BlobStorageClient",
    "BlobStorageProcessor",
    
    # Models
    "Record",
    "Camera", 
    "CalibrationVideo",
    
    # Enums
    "ContainerTypeEnum",
    "L2R_ResultFile",
    "NCP_ResultFile",
    
    # Azure functions
    "AzureManager",
    "AzureStorageClient",
    "AzureBlobStorageProcessor",
]
