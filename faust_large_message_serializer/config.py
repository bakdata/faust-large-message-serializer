from dataclasses import dataclass
from typing import Optional, Callable, Dict, Union

import boto3
from azure.storage.blob import BlobServiceClient

from faust_large_message_serializer.blob_storage.amazon_blob_storage import (
    AmazonS3Client,
)
from faust_large_message_serializer.blob_storage.azure_blob_storage import (
    AzureBlobStorageClient,
)
from faust_large_message_serializer.blob_storage.blob_storage import BlobStorageClient
from faust_large_message_serializer.blob_storage.empty_blob import EmptyBlobStorage
from faust_large_message_serializer.clients.retrieving_client import RetrievingClient
from faust_large_message_serializer.clients.storing_client import StoringClient
from faust_large_message_serializer.utils.uri_parser import URIParser


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
            AmazonS3Client.PROTOCOL: self.__create_s3_client,
            AzureBlobStorageClient.PROTOCOL: self.__create_azure_blob_storage_client,
        }

        self.__client = None

    def __get_blob_storage_client(self) -> BlobStorageClient:
        schema, _, _ = (
            self.base_path.parse_uri() if self.base_path is not None else (None,) * 3
        )
        return self.__create_blob_storage_client(schema)

    def __create_blob_storage_client(self, schema: str):
        try:
            self.__client = (
                self.__client
                or self._factory_client.get(schema, self.__create_empty_blob_client())()
            )
            return self.__client
        except KeyError as e:
            raise ValueError(
                f"The schema {schema} is not supported at the moment"
            ) from e

    def __create_empty_blob_client(self) -> BlobStorageClient:
        return EmptyBlobStorage()

    def __create_s3_client(self) -> BlobStorageClient:
        s3_config = {
            "aws_secret_access_key": self.large_message_s3_secret_key,
            "aws_access_key_id": self.large_message_s3_access_key,
            "region_name": self.large_message_s3_region,
        }
        if self.large_message_s3_endpoint:
            s3_config["endpoint_url"] = self.large_message_s3_endpoint

        if self.large_message_blob_storage_custom_config:
            self.large_message_blob_storage_custom_config(s3_config)
        s3_client = boto3.client("s3", **s3_config)
        return AmazonS3Client(s3_client)

    def __create_azure_blob_storage_client(self) -> BlobStorageClient:
        abs_config = {"conn_str": self.large_message_abs_connection_string}
        if self.large_message_blob_storage_custom_config:
            self.large_message_blob_storage_custom_config(abs_config)
        abs_client = BlobServiceClient.from_connection_string(**abs_config)
        return AzureBlobStorageClient(abs_client)

    def create_storing_client(self):
        return StoringClient(
            self.__get_blob_storage_client(), self.base_path, self.max_size
        )

    def create_retrieving_client(self):
        return RetrievingClient(self.__get_blob_storage_client())
