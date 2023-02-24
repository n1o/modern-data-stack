from abc import ABC
import boto3
from typing import Tuple
from dagster import resource
import shutil
import os

class RemoteFileSystem(ABC):

    def put_remote(self, local_file, remote_file):
        if os.path.isdir(local_file):
            shutil.copytree(local_file, remote_file)
        else:
            shutil.copy(local_file, remote_file)

    def copy_to_local(self, remote_file, local_file):
        pass

class S3RemoteFileSystem(RemoteFileSystem):
    def __init__(self) -> None:
        self.client = boto3.client("s3")

    def put_remote(self, local_file, remote_file):
        bucket, key = self._split_remote_path(remote_file)
        self.client.upload_file(local_file, bucket, key)
        

    def copy_to_local(self, remote_file, local_file):
        bucket, key = self._split_remote_path(remote_file)
        self.client.download_file(bucket, key, local_file)


    def _split_remote_path(self, remote_path: str) -> Tuple[str, str]:
        simplified = remote_path.removeprefix("s3://")
        [head, *tail] = simplified.split("/")

        return head, "/".join(tail)


@resource()
def s3_remote_file_system():
    return S3RemoteFileSystem()

@resource
def local_remote_file_system():
    return RemoteFileSystem()