from enum import Enum
import os
from typing import Tuple

class ImageType(Enum):
    CUBEMAP = "cubemap"
    EQUIRECT = "equirect"
    STANDARD = "photo"

prefix_splitter = "_f_"
anonymized_suffix = "_anonymized"
image_extension = "jpg"


def name_image(frame_index: int, img_type: ImageType, is_anonymized: bool, prefix="") -> str:
    img_name = f"{frame_index}_{img_type.value}"
    if prefix:
        img_name = f"{prefix}{prefix_splitter}{img_name}"
    if is_anonymized:
        img_name += anonymized_suffix
    img_name += f".{image_extension}"
    return img_name

def rename_image(image_name: str, img_type: ImageType, is_anonymized: bool,prefix="") -> str:
    frame_name_prefix_split = image_name.split(prefix_splitter)
    if frame_name_prefix_split:
        prefix = frame_name_prefix_split[0]#if there is already a prefix, we keep it and ignore the one that is passed
        image_name = frame_name_prefix_split[1]
    try:
        frame_index = int(image_name.split('_')[0])  # Ensure frame_index is an integer
    except ValueError:
        raise ValueError(f"Invalid image name format: {image_name}")

    img_name = name_image(frame_index, img_type, is_anonymized,prefix)
    return img_name

def get_frame_index_from_image_name(input_image_name: str) -> int:
    base_name = os.path.basename(input_image_name)
    frame_name_prefix_split = base_name.split(prefix_splitter)
    if frame_name_prefix_split:
        image_name = frame_name_prefix_split[1]
    try:
        frame_index = int(image_name.split('_')[0])  # Ensure frame_index is an integer
    except ValueError:
        raise ValueError(f"Invalid image name format: {input_image_name}")
    return frame_index

def extract_name_structure(input_image_name:str)->Tuple[str,ImageType,bool]:
    #keep only basename
    base_name = os.path.basename(input_image_name)
    #remove extension if needed
    file_name_no_extension, extension = os.path.splitext(base_name)
    frame_name_prefix_split = file_name_no_extension.split(prefix_splitter)
    prefix=""
    if frame_name_prefix_split:
        image_name = frame_name_prefix_split[1]
        prefix = frame_name_prefix_split[0]
    splitted = image_name.split('_')
    image_type = ImageType(splitted[1])
    is_anonymized = anonymized_suffix in image_name
    return prefix,image_type,is_anonymized

def set_image_name_as_type(image_name:str, type:ImageType)->str:
    is_anonymized = anonymized_suffix in image_name
    img_name = rename_image(image_name, type, is_anonymized)
    return img_name

def set_image_name_as_anonymized(image_name: str) -> str:
    if anonymized_suffix in image_name:
        return image_name
    name, ext = os.path.splitext(image_name)
    return f"{name}{anonymized_suffix}{ext}"