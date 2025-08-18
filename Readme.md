# NCP File Manager

This project is a file manager for LogiRoad data on Azure Blob Storage.

## Installation

### Install from GitHub (Recommended)

Install directly from the GitHub repository:

```bash
pip install git+https://github.com/mmerlangeRA/ncp_file_manager.git
```

### Install specific version or branch

```bash
# Install a specific version/tag
pip install git+https://github.com/mmerlangeRA/ncp_file_manager.git@v0.1.0

# Install from a specific branch
pip install git+https://github.com/mmerlangeRA/ncp_file_manager.git@main
```

### Development Installation

For development, clone the repository and install in editable mode:

```bash
git clone https://github.com/mmerlangeRA/ncp_file_manager.git
cd ncp_file_manager
pip install -e .
```

### Optional Dependencies

Install with optional dependencies:

```bash
# For development tools
pip install "git+https://github.com/mmerlangeRA/ncp_file_manager.git[dev]"

# For server capabilities
pip install "git+https://github.com/mmerlangeRA/ncp_file_manager.git[server]"

# For both
pip install "git+https://github.com/mmerlangeRA/ncp_file_manager.git[dev,server]"
```

NB to use on a dev branch :

```bash
pip install git+https://github.com/mmerlangeRA/ncp_file_manager.git@dev
```

## Configuration

### Environment Variables

Before using the package, you need to set up the required Azure environment variables. The package uses these variables to connect to your Azure Blob Storage account.

Create a `.env` file in your project root or set these environment variables in your system:

```bash
# Azure Storage Account Configuration
AZURE_ACCOUNT_NAME=your_storage_account_name
AZURE_ACCOUNT_KEY=your_storage_account_key
AZURE_STORAGE_CONNECTION_STRING=your_full_connection_string

# Azure Container Names
AZURE_CONTAINER_RAW=your_raw_container_name
AZURE_CONTAINER_EXTRACTED=your_extracted_container_name
AZURE_CONTAINER_PROCESSED=your_processed_container_name
```

#### How to get these values

1. **Azure Storage Account**: Create an Azure Storage Account in the Azure Portal
2. **Account Name**: Found in your storage account overview
3. **Account Key**: Found in "Access keys" section of your storage account
4. **Connection String**: Also found in "Access keys" section
5. **Container Names**: The names of your blob containers (you can create these in Azure Portal or programmatically)

#### Example `.env` file

```bash
AZURE_ACCOUNT_NAME=mystorageaccount
AZURE_ACCOUNT_KEY=abc123def456...
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;
AZURE_CONTAINER_RAW=raw-videos
AZURE_CONTAINER_EXTRACTED=extracted-data
AZURE_CONTAINER_PROCESSED=processed-results
```

**Important:** Never commit your `.env` file to version control. Add it to your `.gitignore` file.

## Usage

### Basic Usage

```python
from logiroad_ncp_file_manager import NCPFileManager, BlobStorageStructure
from logiroad_ncp_file_manager.azure_functions import AzureManager

# Create a blob storage structure
blob_structure = BlobStorageStructure.from_json_file()

# Initialize the file manager (using the abstract base class)
file_manager = NCPFileManager(blob_structure, instance_id="your_instance")

# Or use the Azure-specific implementation
azure_manager = AzureManager(blob_structure, instance_id="your_instance")

# Use the manager for your operations
# ... your code here
```

### Advanced Usage

```python
from logiroad_ncp_file_manager import (
    NCPFileManager,
    BlobStorageStructure,
    ContainerTypeEnum,
    Record
)

# Create from record
record = Record(network_slug="network1", slot="record1")
blob_structure = BlobStorageStructure.from_record(record)

# Initialize and use
file_manager = NCPFileManager(blob_structure, instance_id="example")
```

This package allows you to easily manage LogiRoad data on Azure Blob Storage in your projects. It provides a clean API for downloading and uploading files, with support for different container types and automatic path management.

## Development with Devcontainer

This project supports development using [Visual Studio Code Dev Containers](https://code.visualstudio.com/docs/remote/containers). To get started:

1. **Prerequisites:**
   - [Docker Desktop](https://www.docker.com/products/docker-desktop/)
   - [Visual Studio Code](https://code.visualstudio.com/)
   - [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) for VS Code.

2. **Open in Dev Container:**
   - Open the project folder in VS Code.
   - A notification will appear suggesting you to "Reopen in Container". Click on it.
   - This will build the Docker image and start the dev container.

3. **Post-creation:**
   - The required Python packages will be automatically installed.
   - You can now develop and debug the application inside the container.

4. **Structure**

We consider there are 3 blob containers:

- raw : for raw videos or gps inputs
- extracted : for extracted information from raw: frames (cubemaps if 360), trajectories, etc
- processed : for final processed information

File structure:

- every network has its own "directory" (prefix)
- every record has its own "directory" within a networks

5. **Vocabulary**

Blobs: paths in the blob container:

- blob name : the blob name in the blob storage container
- relative blob name: the relative blob name within a record "directory" == after removing the record prefix.

Files: local paths
