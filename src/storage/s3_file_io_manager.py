from dagster import IOManager, io_manager
import boto3
from botocore.exceptions import ClientError


class S3FileIoManager(IOManager):

    def __init__(self, bucket) -> None:
        self.bucket = bucket
        super().__init__()()

    def handle_output(self, context, obj):
        s3_client = boto3.client('s3')
        response = s3_client.upload_file(obj, self.bucket, obj)

    def load_input(self, context):
        pass
        return read_csv(self._get_path(context))
