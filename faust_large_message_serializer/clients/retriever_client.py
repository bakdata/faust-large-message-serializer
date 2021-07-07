from typing import Union

from faust_large_message_serializer.blob_storage.blob_storage import BlobStorageClient
from faust_large_message_serializer.blob_storage.blog_storage_factory import BlobStorageFactory
from faust_large_message_serializer.config import LargeMessageSerializerConfig, URIParser
from loguru import logger


class RetrievingClient:

    VALUE_PREFIX = "values"
    KEY_PREFIX = "keys"
    IS_BACKED = b"\x01"
    IS_NOT_BACKED = b"\x00"

    def __init__(
        self,
        config: LargeMessageSerializerConfig,
        factory: BlobStorageFactory,
    ):
        self._config = config
        self._factory = factory

    def retrieve_bytes(self, data: bytes) -> Union[bytes, None]:
        if data is None:
            return None

        if data[0:1] == self.IS_NOT_BACKED:
            return data[1:]

        if data[0:1] != self.IS_BACKED:
            raise ValueError("Message can only be marked as backed or non-backed")

        return self.__retrieve_backed_bytes(data)

    def __retrieve_backed_bytes(self, data: bytes) -> bytes:
        client = self._factory.get_blob_storage_client()
        uri = data[1:].decode()
        uri_parser = URIParser(uri)
        _, bucket, key = uri_parser.parse_uri()
        blob_data = client.get_object(bucket, key)
        logger.debug("Extracted large message from blob storage: {}", uri_parser)
        return blob_data
