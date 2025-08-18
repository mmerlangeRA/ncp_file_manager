from dataclasses import dataclass
from typing import Any, List, Optional

@dataclass
class NCPModel:
    id: int

@dataclass
class Network(NCPModel):
    network_slot: str

@dataclass
class Camera(NCPModel):
    unique_id:str

@dataclass
class Record(NCPModel):
    unique_id:Optional[str] = None
    network_uuid:Optional[str] = None
    network_slug:Optional[str] = None
    name:Optional[str] = None
    type:Optional[str] = None
    srid:Optional[int] = None
    slot:Optional[str] = None
 
@dataclass   
class CalibrationVideo(NCPModel):    
    camera: Camera
    title:str
    blob_name:Optional[str] = None
    url:Optional[str] = None
    type:Optional[str] = None
    intrinsics:Optional[List[Any]]=None
    distortion_coeffs:Optional[List[Any]]=None
    detailed_error:Optional[str] = None