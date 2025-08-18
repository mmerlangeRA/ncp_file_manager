import traceback
from typing import List, Dict, Tuple, Optional
import requests
import os
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import hashlib
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

def setup_requests_session() -> requests.Session:
    """Sets up a requests session with a retry mechanism.

    Returns:
        requests.Session: The configured requests session.
    """
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504, 429],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def verify_file_size(path: str, expected_size: int) -> bool:
    """Verifies the size of a file.

    Args:
        path (str): The path to the file.
        expected_size (int): The expected size of the file in bytes.

    Returns:
        bool: True if the file size matches the expected size, False otherwise.
    """
    actual_size = os.path.getsize(path)
    return actual_size == expected_size

def calculate_file_hash(path: str) -> str:
    """Calculates the SHA256 hash of a file.

    Args:
        path (str): The path to the file.

    Returns:
        str: The SHA256 hash of the file.
    """
    sha256_hash = hashlib.sha256()
    with open(path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def download_blob(blob_client: BlobClient, local_file_path: str, max_concurrency: int = 4, max_retries: int = 3) -> bool:
    """Downloads a single blob with retries and verification.

    Args:
        blob_client (BlobClient): The BlobClient for the blob to download.
        local_file_path (str): The local path to save the blob to.
        max_concurrency (int, optional): The maximum number of parallel connections to use. Defaults to 4.
        max_retries (int, optional): The maximum number of retries. Defaults to 3.

    Returns:
        bool: True if the download is successful, False otherwise.
    """
    temp_path = local_file_path + '.temp'
    for attempt in range(max_retries):
        try:
            # Attempt to download blob stream directly
            download_stream = blob_client.download_blob(max_concurrency=max_concurrency)
            expected_size = download_stream.properties.size  # Blob size from download stream

            # Download in chunks
            with open(temp_path, "wb") as file:
                for chunk in download_stream.chunks():
                    file.write(chunk)

            # Verify file size
            if verify_file_size(temp_path, expected_size):
                os.replace(temp_path, local_file_path)
                #logger.debug(f"Successfully downloaded and verified: {local_file_path}")
                return True
            else:
                logger.warning(f"Size verification failed for {local_file_path}")
                if os.path.exists(temp_path):
                    os.remove(temp_path)
        except Exception as e:
            logger.warning(f"Error downloading {local_file_path} (attempt {attempt + 1}): {e}")
            if os.path.exists(temp_path):
                os.remove(temp_path)
            if attempt == max_retries - 1:
                raise Exception(f"Failed to download {blob_client.blob_name} to {local_file_path} after {max_retries} attempts. {e}")
    return False

def download_blobs_in_parallel(sas_url: str,  blob_names: List[str],
                               download_directory: str, max_workers: int = 4, max_concurrency: int = 4, prefix_to_remove:str="") ->List[str]:
    """Downloads multiple blobs in parallel.

    Args:
        sas_url (str): The SAS URL for the container.
        blob_names (List[str]): The list of blob names to download.
        download_directory (str): The directory to download the blobs to.
        max_workers (int, optional): The maximum number of worker threads. Defaults to 4.
        max_concurrency (int, optional): The maximum number of parallel connections for each download. Defaults to 4.
        prefix_to_remove (str, optional): The prefix to remove from the blob name when creating the local file path. Defaults to "".

    Returns:
        ->List[str]: list of pths of the downloaded blobs.
    """
    logger.debug(f"download_blobs_in_parallel with prefix, nb files= {len(blob_names)} prefix {prefix_to_remove}")
    # Initialize ContainerClient with the account URL
    container_client = ContainerClient.from_container_url(container_url=sas_url)

    os.makedirs(download_directory, exist_ok=True)

    def download_single_blob(blob_name: str, download_directory: str) ->str:
        """Downloads a single blob and saves it to a directory structure derived from its name.

        Args:
            blob_name (str): The name of the blob in Azure Storage.
            download_directory (str): The root directory to save the blob.
        """
        # Remove the "prefix." and split the remaining path into directories
        if prefix_to_remove !="" and prefix_to_remove in blob_name:
            local_blob_path = blob_name[len(prefix_to_remove):]
        else:
            local_blob_path = blob_name

        # Construct the full local path
        blob_dir_path, file_name = os.path.split(local_blob_path)
        local_path = os.path.join(download_directory, blob_dir_path, file_name)

        # Ensure directories exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        # Download the blob
        blob_client = container_client.get_blob_client(blob_name)
        download_blob(blob_client, local_path, max_concurrency)
        return local_path

    output_paths = []
    failed_downloads = []
    error =""
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_single_blob, blob_name, download_directory): blob_name for blob_name in blob_names}
        for future in as_completed(futures):
            blob_name = futures[future]
            try:
                local_path = future.result()
                output_paths.append(local_path)
            except Exception as e:
                failed_downloads.append(blob_name)
                error = str(e)
                raise Exception(f"Failed to download {blob_name}: {error}")
    return output_paths

def list_blob_names_with_prefix(sas_url: str, prefix: str) -> List[str]:
    """Lists the names of blobs with a specific prefix in a container.

    Args:
        sas_url (str): The SAS URL for the container.
        prefix (str): The prefix to filter the blobs with.

    Returns:
        List[str]: A list of blob names.
    """
    container_client = ContainerClient.from_container_url(container_url=sas_url)
    blob_list = container_client.list_blobs(name_starts_with=prefix)
    return [blob.name for blob in blob_list]

def download_files_with_prefix_parallel(sas_url: str, prefix: str, download_dir: str, max_workers=8)->List[str]:
    """Downloads all files with a specific prefix from a container in parallel.

    Args:
        sas_url (str): The SAS URL for the container.
        prefix (str): The prefix to filter the blobs with.
        download_dir (str): The directory to download the files to.
        max_workers (int, optional): The maximum number of worker threads. Defaults to 8.

    Returns:list of paths the downloaded files 
    """
    os.makedirs(download_dir, exist_ok=True)

    # List blobs with the specified prefix
    blob_names= list_blob_names_with_prefix(sas_url, prefix)
    return download_blobs_in_parallel(sas_url=sas_url, blob_names=blob_names, download_directory=download_dir, max_workers=max_workers,prefix_to_remove= prefix)
