import io
from functools import lru_cache
from typing import Protocol

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from fabric.connection import Connection


class RemoteFileSystem(Protocol):
    def read_bytes(self, path: str) -> bytes: ...


class S3FileSystem:
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str, bucket: str):
        self._s3_client: BaseClient = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        self._bucket = bucket

    @lru_cache
    def read_bytes(self, path: str) -> bytes:
        with io.BytesIO() as stream:
            try:
                self._s3_client.download_fileobj(
                    Bucket=self._bucket, Key=path, Fileobj=stream
                )
            except ClientError as e:
                raise ValueError(
                    f"No data on S3: key: {path}, bucket: {self._bucket}, msg: {e}"
                ) from e

            stream.seek(0)
            output = stream.read()

            return output


class SftpFileSystem:
    def __init__(self, host: str, username: str, password: str):
        self._host = host
        self._username = username
        self._password = password

    @lru_cache
    def read_bytes(self, path: str) -> bytes:
        with Connection(
            host=self._host,
            user=self._username,
            connect_kwargs={
                "password": self._password,
                "allow_agent": False,
                "look_for_keys": False,
            },
        ) as conn:
            sftp = conn.sftp()
            with sftp.file(path, "r") as f:
                return f.read()
