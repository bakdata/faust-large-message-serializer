from typing import Union
from uuid import uuid4

from loguru import logger

from faust_large_message_serializer.config import LargeMessageSerializerConfig


class StoringClient:

    VALUE_PREFIX = "values"
    KEY_PREFIX = "keys"
    IS_BACKED = b"\x01"
    IS_NOT_BACKED = b"\x00"

    def __init__(
        self,
        config: LargeMessageSerializerConfig
    ):
        self._config = config

    def store_bytes(self, topic: str, data: bytes, is_key: bool) -> Union[bytes, None]:
        if data is None:
            return None

        if self.__needs_backing(data):
            key = self.__create_blob_storage_key(topic, is_key)
            uri = self.__upload_to_blob_storage(key, data)
            return self.__serialize(uri, self.IS_BACKED)
        else:
            return self.__serialize(data, self.IS_NOT_BACKED)

    def __create_blob_storage_key(self, topic: str, is_key: bool) -> str:
        if self._config.base_path is None:
            raise ValueError("Base path must not be null")
        prefix = self.KEY_PREFIX if is_key else self.VALUE_PREFIX
        schema, bucket, path = self._config.base_path.parse_uri()
        random_id = str(uuid4())
        storage_accumulated_path = [path, topic, prefix, random_id]
        storage_path = "/".join(filter(None, storage_accumulated_path))
        return storage_path

    def __needs_backing(self, data: bytes) -> bool:
        if len(data) > self._config.max_size:
            return True
        return False

    def __upload_to_blob_storage(self, key: str, data: bytes) -> str:
        client = self._config.get_blob_storage_client()
        _, bucket, _ = self._config.base_path.parse_uri()
        uri = client.put_object(data, bucket, key)
        logger.debug("Stored large message on blob storage: {}", uri)
        return uri

    def __serialize(self, uri: Union[bytes, str], flag: bytes) -> bytes:
        data_bytes = uri if isinstance(uri, bytes) else uri.encode("utf-8")
        return flag + data_bytes
