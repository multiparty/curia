"""
Helper class for get_data and put_data
Establishes swift connection and returns a connection object
"""
import os
from google.cloud import storage
from google.oauth2 import service_account


class GCPHandler:
    def __init__(self, config):
        """
        Returns a Swift connection object
        """
        if not config:
            self.storage_client = storage.Client()
        else:
            cred_path = config['GOOGLE_APPLICATION_CREDENTIALS'] or os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            credentials = service_account.Credentials.from_service_account_file(cred_path)
            self.storage_client = storage.Client(credentials=credentials)


class GCPData:
    """
    Upload files to a swift container, download files from a container, and create a container.
    """

    def __init__(self, config=None):
        self.gcp_client = GCPHandler(config).storage_client
        self.cwd = os.getcwd()

    def create_bucket(self, bucket_name):
        """
        Create a new bucket in specific location with storage class
        """
        bucket = self.gcp_client.bucket(bucket_name)
        bucket.storage_class = "COLDLINE"
        new_bucket = self.gcp_client.create_bucket(bucket, location="us")

        print("Created bucket {} in {} with storage class {}".format(new_bucket.name, new_bucket.location, new_bucket.storage_class))
        return new_bucket

    def get_data(self, bucket_name, file_path, out_file):
        """
        Downloads a blob from the bucket
        """
        bucket = self.gcp_client.bucket(bucket_name)

        # Construct a client side representation of a blob.
        # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
        # any content from Google Cloud Storage. As we don't need additional data,
        # using `Bucket.blob` is preferred here.
        blob = bucket.blob(file_path)
        blob.download_to_filename(out_file)

        print("Blob {} downloaded to {}.".format(file_path, out_file))

    def get_all_data(self, bucket_name):
        """
        Retrieve all data from a container into a local file
        """

        objects = self.gcp_client.list_blobs(bucket_name)
        for blob in objects:
            out_file = self.cwd + '/' + blob.name
            self.get_data(bucket_name, blob.name, out_file)

    def put_data(self, bucket_name, local_file_path, dest_file_path):
        """
        Uploads a file to the bucket.
        """

        buckets = self.gcp_client.list_buckets()
        names = [b.name for b in buckets]

        if bucket_name not in names:
            bucket = self.create_bucket(bucket_name)
        else:
            bucket = self.gcp_client.bucket(bucket_name)

        blob = bucket.blob(dest_file_path)

        blob.upload_from_filename(local_file_path)

        print("File {} uploaded to {}.".format(local_file_path, dest_file_path))
