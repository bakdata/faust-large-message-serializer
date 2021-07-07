from functools import lru_cache
from typing import Optional, Callable, Dict, Tuple, Union
from urllib.parse import urlparse

from dataclasses import dataclass

import boto3
from azure.storage.blob import BlobServiceClient
from faust_s3_backed_serializer import LargeMessageSerializerConfig

from faust_large_message_serializer.blob_storage.amazon_blob_storage import (
    AmazonS3Client,
)
from faust_large_message_serializer.blob_storage.azure_blob_storage import (
    AzureBlobStorageClient,
)
from faust_large_message_serializer.blob_storage.blob_storage import BlobStorageClient


@dataclass
class URIParser:
    base_path: str

    def __post_init__(self):
        self.base_path = self.base_path.lower()

    def parse_uri(self) -> Tuple[str, str, str]:
        result = urlparse(self.base_path)
        scheme = result.scheme
        bucket = result.netloc
        if len(result.path) != 0 and result.path[0] == "/":
            path = result.path[1:]
        else:
            path = result.path
        return scheme, bucket, path

    def __repr__(self):
        return self.base_path


@dataclass
class LargeMessageSerializerConfig:
    base_path: Optional[Union[str, URIParser]] = None
    max_size: int = 1000 * 1000
    large_message_s3_secret_key: Optional[str] = None
    large_message_s3_access_key: Optional[str] = None
    large_message_s3_region: Optional[str] = None
    large_message_s3_endpoint: Optional[str] = None
    large_message_abs_connection_string: Optional[str] = None
    large_message_blob_storage_custom_config: Optional[
        Callable[[Dict[str, Optional[str]]], None]
    ] = None

    def __post_init__(self):
        self.base_path = (
            URIParser(self.base_path)
            if isinstance(self.base_path, str)
            else self.base_path
        )

        self._factory_client = {
            AmazonS3Client.PROTOCOL: _create_s3_client,
            AzureBlobStorageClient.PROTOCOL: _create_azure_blob_storage_client,
        }

        self.__client = None

    def get_blob_storage_client(self) -> BlobStorageClient:
        schema, _, _ = self.base_path.parse_uri()
        return self.__create_storage_client(schema, self)

    def __create_storage_client(self, schema: str, config):
        try:
            self.__client = self.__client or self._factory_client[schema](config)
            return self.__client
        except KeyError as e:
            raise ValueError(
                f"The schema {schema} is not supported at the moment"
            ) from e


def _create_s3_client(config: LargeMessageSerializerConfig) -> BlobStorageClient:
    s3_config = {
        "aws_secret_access_key": config.large_message_s3_secret_key,
        "aws_access_key_id": config.large_message_s3_access_key,
        "region_name": config.large_message_s3_region,
    }
    if config.large_message_s3_endpoint:
        s3_config["endpoint_url"] = config.large_message_s3_endpoint

    if config.large_message_blob_storage_custom_config:
        config.large_message_blob_storage_custom_config(s3_config)
    s3_client = boto3.client("s3", **s3_config)
    return AmazonS3Client(s3_client)


def _create_azure_blob_storage_client(
    config: LargeMessageSerializerConfig,
) -> BlobStorageClient:
    abs_config = {"conn_str": config.large_message_abs_connection_string}
    if config.large_message_blob_storage_custom_config:
        config.large_message_blob_storage_custom_config(abs_config)
    abs_client = BlobServiceClient.from_connection_string(**abs_config)
    return AzureBlobStorageClient(abs_client)
