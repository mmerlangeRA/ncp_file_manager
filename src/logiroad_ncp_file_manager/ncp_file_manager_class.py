import logging
import os
import shutil
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple

from .models import Record
from .const import ContainerTypeEnum,L2R_ResultFile, NCP_ResultFile
from .blob_storage_structure import BlobStorageStructure, ContainerBase, L2R_ResultFileNameManager
from .blob_storage_client import BlobStorageClient

logger = logging.getLogger(__name__)

tmp_dir = "tmp"

def get_temp_dir(instanceId: str):
    base_dir = os.path.join(tmp_dir, f"gopro_{instanceId}")
    os.makedirs(base_dir, exist_ok=True)
    return base_dir

class NCPFileManager(ABC):
    """Manages Cloud blob storage operations for a given record."""
    blobStorageStructure:BlobStorageStructure
    blobStorageClient:BlobStorageClient
    tmp_dir = ""
    input_dir = ""
    output_dir = ""
    downloaded_ncp_results:Dict[NCP_ResultFile,str]
    processed_ncp_results:Dict[NCP_ResultFile,str]
    processed_l2r_results:Dict[L2R_ResultFile,str]
    l2r_results_blob_names:Dict[L2R_ResultFile,str]
    downloaded_frame_dir = ""
    downloaded_equirect_dir = ""
    record_prefix = ""
    processed_frame_dir=""
    processed_equirect_dir=""
    
    def __init__(self, _blob_storage_structure:BlobStorageStructure | dict, instance_id:str,use_record_dir=True):
        """
        Initializes the NCPFileManager.

        Args:
            _blob_storage_structure (BlobStorageStructure or dict): The blob storage structure configuration.
            instance_id (str): The instance ID for the temporary directory.
            use_record_dir (bool, optional): Whether to use the record prefix in the temp dir. Defaults to True.
        """
        if isinstance(_blob_storage_structure, dict):
            blob_storage_structure: BlobStorageStructure = BlobStorageStructure(**_blob_storage_structure)
        else:
            blob_storage_structure: BlobStorageStructure = _blob_storage_structure
        
        self.blobStorageStructure: BlobStorageStructure = blob_storage_structure
        
        if use_record_dir:
            self.tmp_dir = os.path.join(get_temp_dir(instance_id),self.blobStorageStructure.record_prefix)
        else: 
            self.tmp_dir = get_temp_dir(instance_id)
            
        self.input_dir = os.path.join(self.tmp_dir, "input")
        self.output_dir = os.path.join(self.tmp_dir, "output")
        self.record_prefix = blob_storage_structure.record_prefix
        
        # let's specify default values for these local paths. They could be overriden if needed
        self.downloaded_frame_dir = os.path.join(self.input_dir, "frames")
        self.downloaded_equirect_dir = os.path.join(self.input_dir, "equirect")
        self.processed_frame_dir = os.path.join(self.output_dir, "frames")
        self.processed_equirect_dir = os.path.join(self.output_dir, "equirect")
        self.processed_ncp_results = {}
        self.processed_l2r_results = {}
        self.downloaded_ncp_results = {}
        self.l2r_results_blob_names = {}
        container_extracted = blob_storage_structure.container_extracted
        if container_extracted:
            for result_file in NCP_ResultFile:
                self.processed_ncp_results[result_file] = os.path.join(self.input_dir, container_extracted.ncp_result_blobs[result_file.value])
            for result_file in L2R_ResultFile:
                self.processed_l2r_results[result_file] = os.path.join(self.input_dir, container_extracted.l2r_result_blobs[result_file.value])
        
        #let's create necessary folders
        dirs = [self.tmp_dir, self.input_dir, self.output_dir, self.downloaded_frame_dir, self.downloaded_equirect_dir, self.processed_frame_dir, self.processed_equirect_dir]
        for dir in dirs:
            os.makedirs(dir, exist_ok=True)
        
        self.set_L2R_blob_names(gps_reader_name="")

    def set_L2R_blob_names(self,gps_reader_name:str):
       l2r_result_name_manager= L2R_ResultFileNameManager(gps_reader_name=gps_reader_name)
       for r in L2R_ResultFile:
           self.l2r_results_blob_names[r] = l2r_result_name_manager.get_path(l2r_result_file=r)
    
    @classmethod
    def from_blob_storage_structure(cls, blob_storage_structure_json:Dict | str | BlobStorageStructure , instance_id:str, use_record_dir=True):
        """Creates an NCPFileManager instance from a JSON file.

        Args:
            blob_storage_structure_json (Dict): blob_storage_structure as json
            instance_id (str): The instance ID for the temporary directory.
            use_record_dir (bool, optional): Whether to use the record prefix in the temp dir. Defaults to True.

        Returns:
            NCPFileManager: The NCPFileManager instance.
        """
        if isinstance(blob_storage_structure_json, BlobStorageStructure):
            return cls(blob_storage_structure_json, instance_id, use_record_dir)
            
        return cls(BlobStorageStructure.from_input(blob_storage_structure_json), instance_id, use_record_dir)
    
    def clean(self)->None:
        """Cleans the temporary directory."""
        logging.debug("cleaning tmp dir")
        shutil.rmtree(self.tmp_dir,ignore_errors=True)

    def remove_record_prefix(self, path:str)->str:
        """Removes the record prefix from a given path.
        
        Args:
            path (str): The path to remove the prefix from.
            
        Returns:
            str: The path without the record prefix.
        """
        prefix=self.record_prefix
        if prefix !="" and prefix in path:
            return path[len(prefix):]
        return path
    
    @abstractmethod
    def get_downloaded_blob_name(self, blob_name:str, download_directory:str=None)->str:
        pass
    
    @abstractmethod
    def get_downloaded_paths_of_blobs(self, blob_names:List[str])->List[str]:
        """Gets the local paths for a list of downloaded blobs.
        
        Args:
            blob_names (List[str]): The names of the blobs.
            
        Returns:
            List[str]: The local paths for the downloaded blobs.
        """
        pass
    
    @abstractmethod
    def download_blobs_in_parallel(self, container_type:ContainerTypeEnum, blob_names:List[str], download_directory:str="",max_workers: int = 4, max_concurrency: int = 4, prefix_to_remove:str="")-> List[str]:
        """Downloads a list of blobs in parallel from the specified container.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to download from.
            blob_names (List[str]): The list of blob names to download.
            download_directory (str, optional): The directory to download the blobs to. Defaults to the input directory.
            
        Returns:
            List[str]: list of downloaded paths
        """
        pass
    
    @abstractmethod
    def _download_blobs_with_prefix_parallel(self, container_type:ContainerTypeEnum, prefix:str, download_directory:str="")->List[str]:
        """Downloads files with a specific prefix in parallel from the specified container.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to download from.
            prefix (str): The prefix to filter the blobs with.
            download_directory (str, optional): The directory to download the files to. Defaults to the input directory.
            
        Returns:
            List[str]: list of downloaded file paths.
        """
        pass
    
    @abstractmethod
    def _download_blob(self, container_type:ContainerTypeEnum,blob_name:str, download_directory:str="", remove_prefix=True)-> str:
        """Downloads a single blob from the specified container.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to download from.
            blob_name (str): The name of the blob to download.
            download_directory (str, optional): The directory to download the blob to. Defaults to the input directory.
            
        Returns:
            str: The local path of the downloaded blob.
        """
        pass

    @abstractmethod
    def download_calibration_video(self, container_type:ContainerTypeEnum,blob_name:str, download_directory:str="")-> str:
        """Downloads the calibration video from the specified container.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to download from.
            blob_name (str): The name of the blob to download.
            download_directory (str, optional): The directory to download the video to. Defaults to the input directory.
            
        Returns:
            str: The local path of the downloaded calibration video.
        """
        pass
    
    @abstractmethod
    def upload_file_to_cloud_record_directory(self,container_type:ContainerTypeEnum,file_path:str, blob_name:str, sas_url:str)-> str:
        pass
    
    @abstractmethod
    def upload_folder_to_cloud_parallel(self, container_type:ContainerTypeEnum,local_folder_path:str, output_blob_prefix:str, remove_files:bool = False)-> List[str]:
        """Uploads a local folder to Cloud in parallel.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to upload to.
            local_folder_path (str): The path of the local folder to upload.
            output_blob_prefix (str): The prefix for the blobs in the container.
            remove_files (bool, optional): Whether to remove the local files after upload. Defaults to False.
            
        Returns:
            List[str]: list of blob names.
        """
        pass
    
    @abstractmethod
    def upload_record_folder_to_cloud_parallel(self, container_type:ContainerTypeEnum,local_folder_path:str,blob_prefix="",remove_files:bool = False)-> List[str]:
        """Uploads a local folder to Cloud in parallel. We add the record prefix to the blob name
        
        Args:
            container_type (ContainerTypeEnum): The type of container to upload to.
            local_folder_path (str): The path of the local folder to upload.
            remove_files (bool, optional): Whether to remove the local files after upload. Defaults to False.
            
        Returns:
            List[str]: list of blob names.
        """
        pass
    
    @abstractmethod
    def upload_record_file_to_cloud(self, container_type:ContainerTypeEnum,file_to_upload_path:str, blob_name:str, remove_file:bool = False)-> str:
        """Uploads a single record file to Cloud.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to upload to.
            file_to_upload_path (str): The path of the local file to upload.
            blob_name (str): The name of the blob in the container.
            remove_file (bool, optional): Whether to remove the local file after upload. Defaults to False.
            
        Returns:
            str:  blob name.
        """
        pass
    
    def download_ncp_result_file(self,container_type:ContainerTypeEnum, result_file:NCP_ResultFile)->str:
        file_name = self.blobStorageStructure.get_NCP_result_blob(container_type=container_type, ncp_result=result_file)
        container_blob_name = self.blobStorageStructure.get_cloud_blob_path_with_record_prefix(file_name)
        downloaded_path= self._download_blob(container_type, container_blob_name, self.input_dir)
        self.downloaded_ncp_results[result_file]= downloaded_path
        return downloaded_path

    def download_frames(self, container_type:ContainerTypeEnum)-> List[str]:
        """Downloads all frames from the specified container.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to download from.
            
        Returns:
            List[str]: list of frame paths.
        """
        frame_prefix = self.blobStorageStructure.get_frame_directory_prefix(container_type=container_type)
        blob_directory_prefix = self.blobStorageStructure.get_cloud_blob_path_with_record_prefix(frame_prefix)
        downloaded_paths= self._download_blobs_with_prefix_parallel(container_type= container_type, prefix=blob_directory_prefix,download_directory= self.downloaded_frame_dir)
        return downloaded_paths
    
    def download_equirects(self, container_type:ContainerTypeEnum)->List[str]:
        """Downloads all equirectangular frames from the specified container.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to download from.
            
        Returns:
            List[str]: list of frame paths.
        """
        equirect_prefix = self.blobStorageStructure.get_equirect_directory_prefix(container_type=container_type)
        downloaded_paths= self._download_blobs_with_prefix_parallel(container_type, equirect_prefix, self.downloaded_equirect_dir)
        return downloaded_paths
    
    def upload_downloaded_ncp_result_file(self, container_type:ContainerTypeEnum, ncp_result:NCP_ResultFile)-> str:
        """Uploads the downloaded result file to the specified container.

        Args:
            container_type (ContainerTypeEnum): The type of container to upload to.
            ncp_result (ResultFile): The result file to upload.

        Returns:
            str:  blob name.
        """
        relative_blob_name = self.blobStorageStructure.get_NCP_result_blob(container_type=container_type,ncp_result=ncp_result)
        return self.upload_record_file_to_cloud(container_type, self.downloaded_ncp_results[ncp_result], relative_blob_name)
    
    def upload_processed_ncp_result_file(self, container_type:ContainerTypeEnum, ncp_result:NCP_ResultFile)-> str:
        """Uploads the processed L2R result file to the specified container.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to upload to.
            ncp_result (NCP_ResultFile): the type of ncp result to upload
            
        Returns:
            str:  blob name.
        """
        ncp_result_path = self.blobStorageStructure.get_NCP_result_blob(container_type=container_type, ncp_result= ncp_result)
        processed_path = self.processed_ncp_results[ncp_result]
        return self.upload_record_file_to_cloud(container_type, processed_path, ncp_result_path)
    
    def upload_processed_ncp_imu(self, container_type:ContainerTypeEnum, imu_file_path:str, target_blob_name)-> str:
        """Uploads the processed NCP IMU data to the specified container.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to upload to.
            imu (IMUTrajectory): The IMU data to upload.
            target_blob_name (str): The name of the target blob.
            
        Returns:
            str:  blob name.
        """
        return self.upload_record_file_to_cloud(container_type, file_to_upload_path=imu_file_path,blob_name=target_blob_name )
    
    def upload_processed_l2r_result_file(self, container_type:ContainerTypeEnum, l2r_result:L2R_ResultFile)-> str:
        """Uploads the processed L2R result file to the specified container.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to upload to.
            l2r_result (L2R_ResultFile): the type of L2R result to upload
            
        Returns:
            str: A tuple containing the success status, details of the upload, and the name of the blob.
        """
        l2r_blob_name = self.l2r_results_blob_names[l2r_result]
        processed_path = self.processed_l2r_results[l2r_result]
        return self.upload_record_file_to_cloud(container_type, processed_path, l2r_blob_name)
    
    def upload_downloaded_frames(self, container_type:ContainerTypeEnum)-> List[str]:
        """Uploads the downloaded frames to the specified container.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to upload to.
            
        Returns:
            List[str]: the uploaded blob names.
        """
        frame_prefix = self.blobStorageStructure.get_frame_directory_prefix(container_type=container_type)
        return self.upload_folder_to_cloud_parallel(container_type, self.downloaded_frame_dir, frame_prefix)
    
    def upload_processed_frames(self, container_type:ContainerTypeEnum, local_dir:str=None)-> List[str]:
        """Uploads the processed frames to the specified container.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to upload to.
            local_dir (str, optional): The local directory containing the frames. Defaults to the processed frames directory.
            
        Returns:
            List[str]: list of blob names
        """
        frame_prefix = self.blobStorageStructure.get_frame_directory_prefix(container_type=container_type)
        
        if local_dir is None or local_dir=="":
            local_dir = self.processed_frame_dir
        return self.upload_record_folder_to_cloud_parallel(container_type=container_type,local_folder_path= local_dir,blob_prefix=frame_prefix)

    def upload_downloaded_equirects(self, container_type:ContainerTypeEnum)-> List[str]:
        """Uploads the downloaded equirectangular frames to the specified container.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to upload to.
            
        Returns:
            List[str]: list of uploaded blob names.
        """
        blobStorageStructure: BlobStorageStructure = self.blobStorageStructure
        equirect_prefix =blobStorageStructure.get_equirect_directory_prefix(container_type)
        return self.upload_record_folder_to_cloud_parallel.json(container_type, self.downloaded_equirect_dir, equirect_prefix)
    
    def upload_processed_equirects(self, container_type:ContainerTypeEnum,extra_prefix="")-> List[str]:
        """Uploads the processed equirectangular frames to the specified container.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to upload to.
            extra_prefix (str, optional): An extra prefix to add to the blob path. Defaults to "".
            
        Returns:
            List[str]:list of uploaded blob names.
        """
        blobStorageStructure: BlobStorageStructure = self.blobStorageStructure
        equirect_prefix = blobStorageStructure.get_equirect_directory_prefix(container_type)
        equirect_prefix = os.path.join(extra_prefix, equirect_prefix)
        return self.upload_record_folder_to_cloud_parallel(container_type, self.processed_equirect_dir, equirect_prefix)

    @abstractmethod
    def get_list_of_blob_names(self, container_type:ContainerTypeEnum, prefix:str="", extensions:List[str]=[])-> List[str]:
        """Gets a list of blob names from the specified container with a given prefix and extensions.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to list from.
            prefix (str, optional): The prefix to filter the blobs with. Defaults to "".
            extensions (List[str], optional): The list of extensions to filter the blobs with. Defaults to [].
            
        Returns:
            List[str]: A list of blob names.
        """

        pass
    
    @abstractmethod
    def get_list_of_frame_blob_names(self, container_type:ContainerTypeEnum, blob_name_should_include_text:str=None)->List[str]:
        """Gets a list of frame blob names from the specified container.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to list from.
            
        Returns:
            List[str]: A list of frame blob names.
        """
        pass
    
    @abstractmethod
    def get_list_of_cubemap_blob_names(self, container_type:ContainerTypeEnum)->List[str]:
        """Gets a list of cubemap blob names from the specified container.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to list from.
            
        Returns:
            List[str]: A list of cubemap blob names.
        """
        pass
    
    @abstractmethod
    def get_list_of_equirect_blob_names(self, container_type:ContainerTypeEnum)->List[str]:
        """Gets a list of equirectangular blob names from the specified container.
        
        Args:
            container_type (ContainerTypeEnum): The type of container to list from.
            
        Returns:
            List[str]: A list of equirectangular blob names.
        """
        pass
    
    def delete_all_files_in_container_for_record(self, container_type:ContainerTypeEnum, record:Record, blob_storage_structure:BlobStorageStructure=None)->bool:
        if blob_storage_structure is None:
            blob_storage_structure = BlobStorageStructure.from_record(record=record)
        
        record_prefix = blob_storage_structure.record_prefix
        
        if record.slot not in record_prefix:
            raise Exception("record not found in passed blob_storage_structure")
        
        container_name = blob_storage_structure.get_container(container_type).name
        self.blobStorageClient.delete_blobs_by_prefix(container_name=container_name, prefix=record_prefix)

    def delete_all_files_in_all_containers_for_record(self, record:Record)->bool:
        for container_type in ContainerTypeEnum:
            self.delete_all_files_in_container_for_record(container_type=container_type, record=record)
    
    def delete_all_files_in_container(self,container_type: ContainerTypeEnum) -> int:
        """
        Deletes all files in the specified container type for the current record structure
        using a batch operation.
        Args:
            container_type (ContainerTypeEnum): The type of container (RAW, EXTRACTED, PROCESSED).
        Returns:
            int: The number of blobs submitted for deletion.
        """
        
        record_prefix = self.blobStorageStructure.record_prefix
        container: ContainerBase = None

        if container_type == ContainerTypeEnum.RAW:
            container = self.blobStorageStructure.container_raw
        elif container_type == ContainerTypeEnum.EXTRACTED:
            container = self.blobStorageStructure.container_extracted
        elif container_type == ContainerTypeEnum.PROCESSED:
            container = self.blobStorageStructure.container_processed
        else:
            # Should not happen if ContainerTypeEnum is used correctly
            raise ValueError(f"Invalid container_type: {container_type}")

        if not container:
            # Should not happen
            raise ValueError(f"Container object not found for type: {container_type}")

        logger.info(f"Attempting to delete files with prefix '{record_prefix}' in container '{container.name}' of type '{container_type.value}'.")
        
        # Use the batch delete method from AzureStorageClient
        deleted_count = self.blobStorageClient.delete_blobs_by_prefix(
            container_name=container.name,
            prefix=record_prefix
        )
        logger.info(f"Submitted {deleted_count} blobs for deletion from container '{container.name}'.")
        return deleted_count
