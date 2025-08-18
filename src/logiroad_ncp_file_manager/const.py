from enum import Enum


class ContainerTypeEnum(Enum):
    RAW = "raw"
    EXTRACTED = "extracted"
    PROCESSED = "processed"

class NCP_ResultFile(Enum):
    NCP_TRAJECTORY = "ncp_trajectory_path"
    NCP_GPS_FRAMES = "ncp_gps_frames_path"
    NCP_GYRO = "ncp_gyro_path"
    NCP_GRAV = "ncp_grav_path"
    NCP_ACCEL = "ncp_accel_path"
    
class L2R_ResultFile(Enum):
    L2R_TRAJECTORY="l2r_trajectory"
    L2R_RESULT="l2r_result"
    L2R_VPNG = "l2r_vpng"

allowed_image_extensions = ['.png','.jpg','.jpeg']