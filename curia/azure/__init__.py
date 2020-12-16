"""
Helper class for get_data and put_data
Establishes swift connection and returns a connection object
"""
import os
from azure.storage.blob import BlobServiceClient


class AzureHandler:
    def __init__(self, config):
        """
        Returns a Swift connection object
        """

        connect_str = config['AZURE_STORAGE_CONNECTION_STRING'] or os.getenv('AZURE_STORAGE_CONNECTION_STRING')

        self.blob_service_client = BlobServiceClient.from_connection_string(connect_str)


class AzureData:
    """
    Upload files to a swift container, download files from a container, and create a container.
    """

    def __init__(self, config):
        self.azure_client = AzureHandler(config).blob_service_client


    def create_container(self, container_name):
        """
        Create a container.
        """
        self.azure_client.create_container(container_name)
        print("Container {0} created.".format(container_name))


    def get_data(self, container_name, file_path, out_file):
        """
        Retrieve data from an existing container to a local file
        """
        with open(out_file, "wb") as download_file:
            blob_client = self.azure_client.get_blob_client(container=container_name, blob=file_path)
            download_file.write(blob_client.download_blob().readall())


    def get_all_data(self, container_name, out_file):
        """
        Retrieve all data from a container into a local file
        """
        container_client = self.azure_client.get_container_client(container_name)
        blob_list = container_client.list_blobs()
        for blob in blob_list:
            self.get_data(container_name, blob, out_file)


    def put_data(self, container_name, file_path):
        """
        Put data into an existing container.
        """

        # Check if container exists first (create it if not)
        containers = list(self.azure_client.list_containers(name_starts_with=container_name))
        if container_name not in containers:
            self.create_container(container_name)

        # Create a blob client using the local file name as the name for the blob
        blob_client = self.azure_client.get_blob_client(container=container_name, blob=file_path)

        # Upload the created file
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data)
            print("\nUploading to Azure Storage as blob:\n\t" + file_path)
