from typing import Union

from faust_s3_backed_serializer.blob_storage.blob_storage import BlobStorageClient
from faust_s3_backed_serializer.config import LargeMessageSerializerConfig, URIParser
from loguru import logger


class RetrieverClient:

    VALUE_PREFIX = "values"
    KEY_PREFIX = "keys"
    IS_BACKED = b"\x01"
    IS_NOT_BACKED = b"\x00"

    def __init__(
        self,
        config: LargeMessageSerializerConfig,
        client: BlobStorageClient,
    ):
        self._config = config
        self._client = client

    def retrieve_bytes(self, data: bytes) -> Union[bytes, None]:
        if data is None:
            return None

        if data[0] == self.IS_NOT_BACKED:
            return data[1:]

        if data[0] != self.IS_BACKED:
            raise ValueError("Message can only be marked as backed or non-backed")

    def __retrieve_backed_bytes(self, data: bytes) -> bytes:
        uri = data.decode()
        uri_parser = URIParser(uri)
        _, bucket, key = uri_parser.parse_uri()
        blob_data = self._client.get_object(bucket, key)
        logger.debug("Extracted large message from blob storage: {}", uri_parser)
        return blob_data
