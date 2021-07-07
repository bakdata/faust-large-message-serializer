from functools import lru_cache
from urllib.parse import urlparse
import boto3
from azure.storage.blob import BlobServiceClient

from faust_large_message_serializer.blob_storage.amazon_blob_storage import (
    AmazonS3Client,
)
from faust_large_message_serializer.blob_storage.azure_blob_storage import (
    AzureBlobStorageClient,
)
from faust_large_message_serializer.blob_storage.blob_storage import BlobStorageClient
from faust_large_message_serializer.config import LargeMessageSerializerConfig


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


class BlobStorageFactory:
    def __init__(self, config: LargeMessageSerializerConfig):
        self._config = config
        self._factory_client = {
            AmazonS3Client.PROTOCOL: _create_s3_client,
            AzureBlobStorageClient.PROTOCOL: _create_azure_blob_storage_client,
        }

    @lru_cache(maxsize=None)
    def get_blob_storage_client(self) -> BlobStorageClient:
        schema, _, _ = self._config.base_path.parse_uri()
        return self.__create_storage_client(schema, self._config)

    def _get_uri_schema(self, uri: str) -> str:
        result = urlparse(uri)
        return result.netloc

    def __create_storage_client(self, schema: str, config):
        try:
            return self._factory_client[schema](config)
        except KeyError as e:
            raise ValueError(f"The schema {schema} is not supported at the moment") from e
