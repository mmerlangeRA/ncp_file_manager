FROM mcr.microsoft.com/azure-functions/python:4-python3.12 AS dependencies

# Set environment variables
ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

# Install Python dependencies (including OpenCV)
COPY requirements.txt .
RUN echo "Installing Python requirements ..." && \
    python3 -m pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    rm requirements.txt

COPY . /home/site/wwwroot

# Set the working directory
WORKDIR /home/site/wwwroot
