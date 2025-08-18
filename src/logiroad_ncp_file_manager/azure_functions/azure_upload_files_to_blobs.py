import logging
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Thread, Lock
from queue import Queue, Empty
from typing import Dict, List, Tuple
from azure.storage.blob import BlobClient

logger = logging.getLogger(__name__)

class BlobData:
    """Data class for blob data."""
    data:List[bytes]
    blob_name=""
    def __init__(self, data:List[bytes], blob_name:str):
        """Initializes the BlobData.

        Args:
            data (List[bytes]): The data of the blob.
            blob_name (str): The name of the blob.
        """
        self.data=data
        self.blob_name = blob_name

def upload_data_to_azure(data_to_upload: bytes, blob_name: str, sas_url: str)->bool:
    """Uploads data to Azure Blob Storage.

    Args:
        data_to_upload (bytes): The data to upload.
        blob_name (str): The name of the blob.
        sas_url (str): The SAS URL for the container.

    Returns:
        bool: True if the upload is successful, False otherwise.
    """
    try:
        # Validate input type
        if not isinstance(data_to_upload, bytes):
            raise TypeError("data_to_upload must be of type bytes.")

        # Append the blob name to the container SAS URL to form the full blob URL
        blob_url = f"{sas_url.split('?')[0]}/{blob_name}?{sas_url.split('?')[1]}"
        # Create a BlobClient
        blob_client = BlobClient.from_blob_url(blob_url)
        # Upload data
        blob_client.upload_blob(data_to_upload, overwrite=True)

        return True
    except Exception as e:
        logger.error(f"Error uploading {blob_name}: {str(e)}")
        return False
    

def upload_file_to_azure(file_to_upload_path: str, blob_name: str, sas_url: str, remove: bool = False)->str:
    """Uploads a file to Azure Blob Storage.

    Args:
        file_to_upload_path (str): The path to the file to upload.
        blob_name (str): The name of the blob.
        sas_url (str): The SAS URL for the container.
        remove (bool, optional): Whether to remove the file after upload. Defaults to False.

    Returns:
        str: The name of the blob if the upload is successful, None otherwise.
    """
    try:
        # Append the blob name to the container SAS URL to form the full blob URL
        blob_url = f"{sas_url.split('?')[0]}/{blob_name}?{sas_url.split('?')[1]}"

        # Create a BlobClient
        blob_client = BlobClient.from_blob_url(blob_url)

        # Upload the file
        with open(file_to_upload_path, "rb") as file:
            blob_client.upload_blob(file, overwrite=True)
        
        # remove the local file
        if remove:
            os.remove(file_to_upload_path)

        return blob_name
    except Exception as e:
       raise Exception(f"Error uploading {blob_name}: {str(e)}")


def upload_folder_to_azure_parallel(
    local_folder_path: str, output_blob_prefix: str, write_sas_url: str, max_workers=8, remove: bool = False
) -> List[str]:
    """Uploads a folder to Azure Blob Storage in parallel.

    Args:
        local_folder_path (str): The path to the local folder to upload.
        output_blob_prefix (str): The prefix for the output blobs.
        write_sas_url (str): The SAS URL for the container.
        max_workers (int, optional): The maximum number of worker threads. Defaults to 8.
        remove (bool, optional): Whether to remove the files after upload. Defaults to False.

    Returns:
        List[str]: the list of blob names.
    """
    uploaded_files = []

    # Collect all files to upload
    files_to_upload = []
    blob_names =[]

    for root, _, files in os.walk(local_folder_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            relative_path = os.path.relpath(file_path, local_folder_path)
            blob_name = relative_path.replace("\\", "/")  # Normalize for Azure
            files_to_upload.append((file_path, blob_name))
            blob_names.append(blob_name)

    # Upload files in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                upload_file_to_azure,
                file_path,
                output_blob_prefix + blob_name,
                write_sas_url,
                remove
            )
            for file_path, blob_name in files_to_upload
        ]

        for future in futures:
            result = future.result()
            if result:
                uploaded_files.append(result)
            else:
                raise Exception(f"Failed to upload {result}")

    return blob_names

def upload_blobs_data_to_azure_parallel(
    blobs_to_upload:List[BlobData], write_sas_url: str, max_workers=8
) -> List[str]:
    """Uploads a list of blobs to Azure Blob Storage in parallel.

    Args:
        blobs_to_upload (List[BlobData]): The list of blobs to upload.
        write_sas_url (str): The SAS URL for the container.
        max_workers (int, optional): The maximum number of worker threads. Defaults to 8.

    Returns:
         List[str]: the list of blob names.
    """
    uploaded_blobs = []
    failed_blobs = []


    # Upload files in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                upload_data_to_azure,
                blob_data.data,
                blob_data.blob_name,
                write_sas_url
            )
            for blob_data in blobs_to_upload
        ]

        for future in futures:
            result = future.result()
            if result:
                uploaded_blobs.append(result)
            else:
                raise Exception(f"Failed to upload {result}")

    

    return [blob.blob_name for blob in blobs_to_upload]

SENTINEL = object()


class BlobUploader:
    """A class for uploading blobs to Azure Blob Storage in the background."""
    def __init__(self, sas_url: str, max_list_len: int = 50, nb_threads: int = 3):
        """Initializes the BlobUploader.

        Args:
            sas_url (str): The SAS URL for the container.
            max_list_len (int, optional): The maximum number of blobs to hold in the queue before switching to parallel upload. Defaults to 50.
            nb_threads (int, optional): The number of threads to use for uploading. Defaults to 3.
        """
        self.sas_url = sas_url
        self.max_list_len = max_list_len
        self.nb_threads = nb_threads

        self._queue = Queue()
        self._threads: List[Thread] = []
        self._lock = Lock()
        self.nb_uploaded = 0
        self.nb_failed = 0

        self.start_thread_upload()

    def __str__(self) -> str:
        """Returns the status of the uploader."""
        return self.status()

    def status(self) -> str:
        """Returns the status of the uploader."""
        return (f"[BlobUploader] Queue: {self._queue.qsize()}, "
                f"Threads: {len(self._threads)}, "
                f"Uploaded: {self.nb_uploaded}, Failed: {self.nb_failed}")

    def upload_byte_frames(self) -> None:
        """Uploads byte frames from the queue."""
        while True:
            blob_data:BlobData = self._queue.get()
            if blob_data is SENTINEL:
                break
            try:
                if upload_data_to_azure(blob_data.data, blob_data.blob_name, self.sas_url):
                    with self._lock:
                        self.nb_uploaded += 1
                else:
                    with self._lock:
                        self.nb_failed += 1
            except Exception as e:
                logger.error(f"Unexpected error during upload: {e}")

    def start_thread_upload(self):
        """Starts the background upload threads."""
        alive = any(t.is_alive() for t in self._threads)
        if alive:
            logger.debug("Upload threads already running.")
            return

        self._threads = []
        for i in range(self.nb_threads):
            t = Thread(target=self.upload_byte_frames)
            t.start()
            self._threads.append(t)
        logger.debug(f"🚀 Started {self.nb_threads} upload threads.")

    def stop_thread_upload(self):
        """Stops the background upload threads."""
        for _ in self._threads:
            self._queue.put(SENTINEL)
        for t in self._threads:
            t.join()
        logger.debug("🛑 All threads stopped.")
        self._threads = []

    def add_blob(self, blob_data: BlobData):
        """Adds a blob to the upload queue."""
        self._queue.put(blob_data)

    def upload_parallel(self) -> List[str]:
        """Uploads the remaining blobs in the queue in parallel."""
        logger.debug(f"⚡ Switching to parallel upload for {self._queue.qsize()} blobs")

        blobs = []
        while not self._queue.empty():
            try:
                blob = self._queue.get_nowait()
                if blob is not SENTINEL:
                    blobs.append(blob)
            except Empty:
                break

        blob_names = upload_blobs_data_to_azure_parallel(blobs, write_sas_url=self.sas_url)

        with self._lock:
            self.nb_uploaded += len(blob_names)

        return blob_names

    def manage(self):
        """Manages the upload queue, switching to parallel upload if the queue gets too long."""
        if self._queue.qsize() > self.max_list_len:
            self.stop_thread_upload()
            self.upload_parallel()
            self.start_thread_upload()

    def stop(self):
        """Stops the uploader."""
        if not self._queue.empty():
            logger.warning(f"⚠️ Warning: stopping but still {self._queue.qsize()} pending")
        self.stop_thread_upload()
   
        
        