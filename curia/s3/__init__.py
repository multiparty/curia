import os
import boto3
from botocore.exceptions import ClientError


class S3Handler:
    """
    Helper class for opening connection to s3 buckets. Handles authentication and configuration.
    """

    def __init__(self, config):
        self.resource = boto3.resource(
            service_name='s3',
            region_name=config['AWS_REGION'] or os.environ['AWS_REGION'],
            aws_access_key_id=config['AWS_ACCESS_KEY_ID'] or os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=config['AWS_SECRET_ACCESS_KEY'] or os.environ['AWS_SECRET_ACCESS_KEY']
        )

        self.client = boto3.client(
            's3',
            aws_access_key_id=config['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=config['AWS_SECRET_ACCESS_KEY']
        )


class S3Data:
    """
    Class for storing and retrieving data from Amazon Simple Storage Service (s3).
    """

    def __init__(self, config):
        self.s3_resource = S3Handler(config).resource
        self.s3_client = S3Handler(config).client

    def create_bucket(self, bucket_name, region=None):
        """Create an S3 bucket in a specified region
        """
        try:
            if region is None:
                self.s3_client.create_bucket(Bucket=bucket_name)
            else:
                location = {'LocationConstraint': region}
                self.s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
            print("Bucket {} created.".format(bucket_name))
        except ClientError as e:
            print(e)

    def get_all_data(self, bucket_name, out_file):
        """
        Retrieve all data from a bucket into a local file
        """
        bucket = self.s3_resource.Bucket(bucket_name)
        for obj in bucket.objects.all():
            self.get_data(bucket_name, obj.key, out_file, append=True)

    def get_data(self, bucket_name, object_name, out_file, append=False):
        """
        Get data from an existing S3 bucket
        """
        # default to overwrite scope, append if necessary
        scope='wb'
        if append:
            scope='ab'

        with open(out_file, scope) as f:
            self.s3_client.download_fileobj(bucket_name, object_name, f)
            print("Object {} downloaded to {}.".format(object_name, out_file))

    def put_data(self, bucket_name, file_name, object_name=None):
        """
        Upload a file to an S3 bucket
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = file_name

        # Upload the file
        try:
            if bucket_name in self.s3_resource.buckets.all():
                self.s3_client.upload_file(file_name, bucket_name, object_name)
            else:
                self.create_bucket(bucket_name)
                self.s3_client.upload_file(file_name, bucket_name, object_name)
            print("File {} uploaded to {}.".format(file_name, object_name))

        except ClientError as e:
            print(e)
