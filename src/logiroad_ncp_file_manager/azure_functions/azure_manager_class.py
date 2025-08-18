import logging
import os
from typing import Dict, List, Tuple

from azure.storage.blob import ContainerClient

from .azure_storage_client import AzureStorageClient

from ..ncp_file_manager_class import NCPFileManager
from ..image_naming import ImageType
from ..const import allowed_image_extensions,ContainerTypeEnum,L2R_ResultFile, NCP_ResultFile
from ..blob_storage_structure import BlobStorageStructure, ContainerBase
from .azure_download_blobs import download_blobs_in_parallel, download_files_with_prefix_parallel
from .azure_upload_files_to_blobs import upload_file_to_azure, upload_folder_to_azure_parallel

logger = logging.getLogger(__name__)


class AzureManager(NCPFileManager):
    """Manages Azure blob storage operations for a given record."""

    def __init__(self, blob_storage_structure:BlobStorageStructure | dict, instance_id:str,use_record_dir=True):
        super().__init__(blob_storage_structure, instance_id, use_record_dir)
        self.blob_storage_client = AzureStorageClient.get_instance()

    def get_downloaded_blob_name(self, blob_name:str, download_directory:str=None)->str:
        prefix_to_remove= self.record_prefix + '/'
        if download_directory is None or download_directory == "":
            download_directory=self.input_dir
        
        if prefix_to_remove !="" and prefix_to_remove in blob_name:
            blob_path = blob_name[len(prefix_to_remove):]
        else:
            blob_path = blob_name
        local_file_path = os.path.join(download_directory, blob_path)
        return local_file_path
    
    def get_downloaded_paths_of_blobs(self, blob_names:List[str])->List[str]:
        """Gets the local paths for a list of downloaded blobs.
        
        Args:
            blob_names (List[str]): The names of the blobs.
            
        Returns:
            List[str]: The local paths for the downloaded blobs.
        """
        return [self.get_downloaded_blob_name(path) for path in blob_names]
    
    def download_blobs_in_parallel(self, container_type:ContainerTypeEnum, blob_names:List[str], download_directory:str="",max_workers: int = 4, max_concurrency: int = 4, prefix_to_remove:str="")->List[str]:
        """Downloads a list of blobs in parallel from the specified container.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to download from.
            blob_names (List[str]): The list of blob names to download.
            download_directory (str, optional): The directory to download the blobs to. Defaults to the input directory.
            
        Returns:
            List[str]: list of paths of downloaded blobs
        """
        if download_directory=="":
            download_directory=self.input_dir
        sas_url = self.blob_storage_structure.get_container(container_type).sas_url
        return download_blobs_in_parallel(sas_url, blob_names, download_directory,max_workers=max_workers, prefix_to_remove=prefix_to_remove)

    def _download_blobs_with_prefix_parallel(self, container_type:ContainerTypeEnum, prefix:str, download_directory:str="")->List[str]:
        """Downloads blobs with a specific prefix in parallel from the specified container.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to download from.
            prefix (str): The prefix to filter the blobs with.
            download_directory (str, optional): The directory to download the blobs to. Defaults to the input directory.
            
        Returns:
            List[str]: list of paths of downloaded blobs.
        """
        if download_directory=="":
            download_directory=self.input_dir
        sas_url = self.blob_storage_structure.get_container(container_type).sas_url
        return download_files_with_prefix_parallel(sas_url, prefix, download_directory)
    
    def _download_blob(self, container_type:ContainerTypeEnum,blob_name:str, download_directory:str="", remove_prefix=True)-> str:
        """Downloads a single blob from the specified container.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to download from.
            blob_name (str): The name of the blob to download.
            download_directory (str, optional): The directory to download the blob to. Defaults to the input directory.
            remove_prefix: if True we keep only the basename
        Returns:
            str: The local path of the downloaded blob.
        """
        if download_directory=="":
            download_directory=self.input_dir
        if remove_prefix:
            prefix_to_remove= self.record_prefix
        
        downloaded_paths=  self.download_blobs_in_parallel(container_type, [blob_name], download_directory,prefix_to_remove=prefix_to_remove)
        return downloaded_paths[0]


    def download_calibration_video(self, container_type:ContainerTypeEnum,blob_name:str, download_directory:str="")-> str:
        """Downloads the calibration video from the specified container.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to download from.
            blob_name (str): The name of the blob to download.
            download_directory (str, optional): The directory to download the video to. Defaults to the input directory.
            
        Returns:
            str: The local path of the downloaded calibration video.
        """
        if download_directory=="":
            download_directory=self.input_dir
        
        downloaded_paths=  self.download_blobs_in_parallel(container_type, [blob_name], download_directory)
        return downloaded_paths[0]

    
    def upload_file_to_cloud_record_directory(self,container_type:ContainerTypeEnum,file_path:str, blob_name:str, sas_url:str)->str:
        sas_url = self.blob_storage_structure.get_container(container_type=container_type).sas_url
        blob_path = self.blob_storage_structure.get_cloud_blob_path_with_record_prefix(blob_name=blob_name)
        return upload_file_to_azure(file_path, blob_path, sas_url)
    
    def upload_folder_to_cloud_parallel(self, container_type:ContainerTypeEnum,local_folder_path:str, output_blob_prefix:str, remove_files:bool = False)->List[str]:
        """Uploads a local folder to Azure in parallel.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to upload to.
            local_folder_path (str): The path of the local folder to upload.
            output_blob_prefix (str): The prefix for the blobs in the container.
            remove_files (bool, optional): Whether to remove the local files after upload. Defaults to False.
            
        Returns:
            List[str]: list of blob names.
        """
        write_sas_url = self.blob_storage_structure.get_container(container_type).sas_url
        return upload_folder_to_azure_parallel(local_folder_path,output_blob_prefix, write_sas_url,remove=remove_files)
    
    def upload_record_folder_to_cloud_parallel(self, container_type:ContainerTypeEnum,local_folder_path:str,blob_prefix="",remove_files:bool = False)-> List[str]:
        """Uploads a local folder to Azure in parallel. We add the record prefix to the blob name
        
        Args:
            container_type (ContainerTypeEnum): The type of container to upload to.
            local_folder_path (str): The path of the local folder to upload.
            remove_files (bool, optional): Whether to remove the local files after upload. Defaults to False.
            
        Returns:
            List[str]: list of blob names.
        """
        output_blob_prefix= self.blob_storage_structure.record_prefix
        output_blob_prefix=os.path.join(output_blob_prefix, blob_prefix).replace("\\","/")
        return self.upload_folder_to_cloud_parallel(container_type=container_type,local_folder_path=local_folder_path,output_blob_prefix=output_blob_prefix,remove_files=remove_files)
    
    def upload_record_file_to_cloud(self, container_type:ContainerTypeEnum,file_to_upload_path:str, blob_name:str, remove_file:bool = False)-> List[str]:
        """Uploads a single record file to Azure.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to upload to.
            file_to_upload_path (str): The path of the local file to upload.
            blob_name (str): The name of the blob in the container.
            remove_file (bool, optional): Whether to remove the local file after upload. Defaults to False.
            
        Returns:
            List[str]: list of blob names.
        """
        sas_url = self.blob_storage_structure.get_container(container_type).sas_url
        blob_name= upload_file_to_azure(file_to_upload_path, self.record_prefix+blob_name, sas_url, remove=remove_file)
        return blob_name
      


    def get_list_of_blob_names(self, container_type:ContainerTypeEnum, prefix:str="", extensions:List[str]=[])-> List[str]:
        """Gets a list of blob names from the specified container with a given prefix and extensions.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to list from.
            prefix (str, optional): The prefix to filter the blobs with. Defaults to "".
            extensions (List[str], optional): The list of extensions to filter the blobs with. Defaults to [].
            
        Returns:
            List[str]: A list of blob names.
        """

        sas_url = self.blob_storage_structure.get_container(container_type).sas_url
        
        container_client = ContainerClient.from_container_url(
            container_url=sas_url
        )

        blob_list = container_client.list_blobs(
            name_starts_with=prefix
        )
        blob_names = [
            blob.name
            for blob in blob_list
            if os.path.splitext(blob.name)[1].lower() in extensions
        ]
        return blob_names
    
    def get_list_of_frame_blob_names(self, container_type:ContainerTypeEnum, blob_name_should_include_text:str=None)->List[str]:
        """Gets a list of frame blob names from the specified container.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to list from.
            
        Returns:
            List[str]: A list of frame blob names.
        """
        relative_frame_prefix = self.blob_storage_structure.get_frame_directory_prefix(container_type=container_type)
        frame_prefix = self.blob_storage_structure.get_cloud_blob_path_with_record_prefix(relative_frame_prefix)
        blob_list = self.get_list_of_blob_names(container_type, frame_prefix, allowed_image_extensions)
        if not blob_name_should_include_text:
            return blob_list
        
        blob_names = [
            blob_name
            for blob_name in blob_list
            if blob_name_should_include_text in blob_name
        ]
        return blob_names

    def get_list_of_cubemap_blob_names(self, container_type:ContainerTypeEnum)->List[str]:
        """Gets a list of cubemap blob names from the specified container.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to list from.
            
        Returns:
            List[str]: A list of cubemap blob names.
        """
        relative_frame_prefix = self.blob_storage_structure.get_frame_directory_prefix(container_type=container_type)
        frame_prefix = self.blob_storage_structure.get_cloud_blob_path_with_record_prefix(relative_frame_prefix)
        blob_list= self.get_list_of_blob_names(container_type, frame_prefix, allowed_image_extensions)
        blob_names = [
            blob_name
            for blob_name in blob_list
            if ImageType.CUBEMAP.value in blob_name
        ]
        return blob_names
    
    def get_list_of_equirect_blob_names(self, container_type:ContainerTypeEnum)->List[str]:
        """Gets a list of equirectangular blob names from the specified container.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to list from.
            
        Returns:
            List[str]: A list of equirectangular blob names.
        """
        relative_equirect_prefix = self.blob_storage_structure.get_equirect_directory_prefix(container_type=container_type)
        equirect_prefix = self.blob_storage_structure.get_cloud_blob_path_with_record_prefix(relative_equirect_prefix)
        blob_list= self.get_list_of_blob_names(container_type, equirect_prefix, allowed_image_extensions)
        blob_names = [
            blob_name
            for blob_name in blob_list
            if ImageType.EQUIRECT .value in blob_name
        ]
        return blob_names
    
    def check_blob_exists(self, container:ContainerBase,blob_name:str)->bool:
        return self.azure_storage_Client.check_blob_exists(container.name,blob_name)
    
    def get_blob_size(self,container:ContainerBase,blob_name:str)->int:
        container_client = self.azure_storage_Client.get_container_client(container_name=container.name)
        blob_client = container_client.get_blob_client(blob_name)
        properties = blob_client.get_blob_properties()
        return properties.size
    
