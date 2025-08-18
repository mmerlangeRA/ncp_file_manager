import json
import logging
import os
import re

from datetime import datetime
from pathlib import Path
from typing import List, Self
from pydantic import BaseModel, Field

from .const import ContainerTypeEnum, L2R_ResultFile, NCP_ResultFile
from .models import CalibrationVideo, Camera, Record
from .settings import *

logger = logging.getLogger(__name__)


def _get_l2r_timestamp_prefix(gps_device_name: str) -> str:
    """
    Extracts a timestamp prefix from the GPS device filename.
    Falls back to the current time if the pattern is not found.
    
    Example: "20230101_120000_device.gps" -> "20230101_120000"
    """
    match = re.match(r"^(\d{8}_\d{6})", gps_device_name)
    if match:
        return match.group(1)
    
    logger.warning(
        f"Could not extract timestamp from '{gps_device_name}'. "
        "Falling back to current timestamp for L2R filename."
    )
    return datetime.now().strftime("%Y%m%d_%H%M%S")


class L2R_ResultFileNameManager:
    custom_l2r_result_path:str
    custom_l2r_trajectory_path:str
    custom_l2r_vpng_path:str
    def __init__(self, gps_reader_name:str):
        timestamp_prefix = _get_l2r_timestamp_prefix(gps_reader_name)
        self.custom_l2r_result_path = f"{timestamp_prefix}_l2r_result.json"
        self.custom_l2r_trajectory_path = f'{Path(gps_reader_name).stem}_l2r_trajectory.csv'
        self.custom_l2r_vpng_path = 'l2r_vpng.vpng'
    
    def get_path(self,l2r_result_file:L2R_ResultFile)->str:
        if l2r_result_file == L2R_ResultFile.L2R_RESULT:
            return self.custom_l2r_result_path
        elif l2r_result_file == L2R_ResultFile.L2R_TRAJECTORY:
            return self.custom_l2r_trajectory_path
        elif l2r_result_file == L2R_ResultFile.L2R_VPNG:
            return self.custom_l2r_vpng_path
        else:
            raise ValueError(f"Not implemented L2R result file: {l2r_result_file}")

class ContainerBase(BaseModel):
    """Base model for Azure container configuration."""
    name:str
    sas_url:str
    model_config = {
        "extra": "ignore"
    }

    @classmethod
    def from_json(cls, json_data):
        return cls(**json_data)

    def to_dict(self):
        return self.model_dump()
    
class ContainerRaw(ContainerBase):
    """Model for the raw container configuration."""
    videos:list[str]
    gps_device_path:str
    
class ContainerExtracted(ContainerBase):
    """Model for the extracted container configuration."""
    l2r_result_path:str=Field(default="l2r_result.json")
    l2r_trajectory_path:str=Field(default="l2r_trajectory.csv") 
    frame_prefix:str
    ncp_result_blobs:dict[str, str]
    l2r_result_blobs:dict[str, str]
    
class ContainerProcessed(ContainerExtracted):
    """Model for the processed container configuration."""
    equirect_prefix:str

class BlobStorageStructure(BaseModel):
    """Model for the Cloud structure configuration."""
    calibration_video:str
    network_prefix:str
    record_prefix:str
    container_raw:ContainerRaw
    container_extracted:ContainerExtracted
    container_processed:ContainerProcessed
    model_config = {
        "extra": "ignore"
    }

    @property
    def containers(self)->List[ContainerBase]:
        return [self.container_raw, self.container_extracted, self.container_processed]
        
    def to_dict(self)->dict:
        """Converts the model to a dictionary."""
        return self.model_dump()
    
    def to_json(self):
        """Converts the model to a JSON string."""
        return json.dumps(self.model_dump(), indent=4)
    
    def get_container(self,container_type:ContainerTypeEnum)->ContainerRaw:
        if container_type == ContainerTypeEnum.RAW:
            return self.container_raw
        elif container_type == ContainerTypeEnum.EXTRACTED:
            return self.container_extracted
        elif container_type == ContainerTypeEnum.PROCESSED:
            return self.container_processed
        else:
            raise ValueError(f"Invalid container type: {container_type}")
    
    def get_NCP_result_blob(self,container_type:ContainerTypeEnum,ncp_result:NCP_ResultFile)->str:
        if container_type == ContainerTypeEnum.RAW:
           raise ValueError(f"Invalid container type, no results: {container_type}")
        container:ContainerExtracted = self.get_container(container_type)
        return container.ncp_result_blobs.get(ncp_result.value)
    
    def get_L2R_result_blob(self, l2r_result_file_manager:L2R_ResultFileNameManager, result_type:L2R_ResultFile)->str:
        return l2r_result_file_manager.get_path(result_type)
    
    def get_cloud_blob_path_with_record_prefix(self,blob_name:str)->str:
        if self.record_prefix is None:
            raise ValueError(f"record prefix not set yet")
        return os.path.join(self.record_prefix, blob_name)
    
    def get_frame_directory_prefix(self,container_type:ContainerTypeEnum)->str:
        """
        Returns the frame directory prefix, relative to the record prefix
        """
        if container_type == ContainerTypeEnum.RAW:
           raise ValueError(f"Invalid container type, no results: {container_type}")
        container:ContainerExtracted = self.get_container(container_type)
        return container.frame_prefix
    
    def get_equirect_directory_prefix(self,container_type:ContainerTypeEnum)->str:
        """
        Returns the equirect frame directory prefix, relative to the record prefix
        """
        if container_type != ContainerTypeEnum.PROCESSED:
           raise ValueError(f"Invalid container type, no equirect possible: {container_type}")
        container:ContainerProcessed = self.get_container(container_type)
        return container.equirect_prefix
    

    @classmethod
    def from_input(cls, data_input: str | dict)->Self: 
        """
        Creates a model instance from either a JSON string or a dictionary.
        This correctly uses Pydantic's underlying validation methods.
        """
        if isinstance(data_input, str):
            # Use the optimized JSON validator
            return cls.model_validate_json(data_input)
        elif isinstance(data_input, dict):
            # Use the standard dictionary validator
            return cls.model_validate(data_input)
        else:
            raise TypeError(f"Input must be a JSON string or a dictionary, not {type(data_input)}")
    
    @classmethod
    def from_json_file(cls, json_path:str=default_blob_storage_structure_template_path)->Self:
        template_path = os.path.join(os.path.dirname(__file__), json_path)
        with open(template_path) as json_data:
            json_data=json_data.read()
            containers:dict = AZURE_CONTAINERS

            json_data = json_data.replace('{container_raw_name}',containers.get("raw"))
            json_data = json_data.replace('{container_extracted_name}',containers.get("extracted"))
            json_data = json_data.replace('{container_processed_name}',containers.get("processed"))

            return cls.from_input(json_data)
    
    @classmethod
    def from_record(cls,record:Record, json_path=default_blob_storage_structure_template_path)->Self:
        template_path = os.path.join(os.path.dirname(__file__), json_path)

        with open(template_path) as json_data:
            json_data=json_data.read().replace(
            '{network_slot}', record.network_slug
            ).replace(
                '{record_slot}', record.slot
            )
            containers:dict = AZURE_CONTAINERS
            json_data = json_data.replace('{container_raw_name}',containers.get("raw"))
            json_data = json_data.replace('{container_extracted_name}',containers.get("extracted"))
            json_data = json_data.replace('{container_processed_name}',containers.get("processed"))

            return cls.from_input(json_data)

    @classmethod
    def from_camera(cls,camera:Camera, json_path=default_blob_storage_structure_template_path)->Self:
        template_path = os.path.join(os.path.dirname(__file__), json_path)

        with open(template_path) as json_data:
            json_data=json_data.read().replace(
            '{camera_id}', camera.unique_id
            )
            containers:dict = AZURE_CONTAINERS

            json_data = json_data.replace('{container_raw_name}',containers.get("raw"))
            json_data = json_data.replace('{container_extracted_name}',containers.get("extracted"))
            json_data = json_data.replace('{container_processed_name}',containers.get("processed"))
            return cls.from_input(json_data)

    @classmethod
    def from_calibration_video(cls,calibrationVideo:CalibrationVideo, json_path=default_blob_storage_structure_template_path)->Self:
        template_path = os.path.join(os.path.dirname(__file__), json_path)

        with open(template_path) as json_data:
            json_data=json_data.read().replace(
            '{camera_id}', calibrationVideo.camera.unique_id
            ).replace(
            '{video_blob}', calibrationVideo.title
            )

            containers:dict = AZURE_CONTAINERS

            json_data = json_data.replace('{container_raw_name}',containers.get("raw"))
            json_data = json_data.replace('{container_extracted_name}',containers.get("extracted"))
            json_data = json_data.replace('{container_processed_name}',containers.get("processed"))
            return cls.from_input(json_data)

    def to_dict(self):
        # Convert the instance back to a dictionary
        return {
            "calibration_video": self.calibration_video,
            "network_prefix": self.network_prefix,
            "record_prefix": self.record_prefix,
            "container_raw": self.container_raw.to_dict(),
            "container_extracted": self.container_extracted.to_dict(),
            "container_processed": self.container_processed.to_dict()
        }

    def to_json(self):
        # Convert the instance to a JSON string
        return json.dumps(self.to_dict(), indent=4)
    
    def get_network_path(self, network_slot:str)->str:
        """Get the container for a network"""
        return self.network_prefix.format(network_slot=network_slot)
    
    def get_record_path(self, network_slot:str, record_slot:str)->str:
        """Get the container path for a record"""
        return self.record_prefix.format(network_slot=network_slot, record_slot=record_slot)

