from faust.serializers.codecs import Codec

from faust_s3_backed_serializer.blob_storage.blog_storage_factory import (
    BlobStorageFactory,
)
from faust_s3_backed_serializer.clients.retriever_client import RetrieverClient
from faust_s3_backed_serializer.clients.storage_client import StorageClient
from faust_s3_backed_serializer.config import LargeMessageSerializerConfig


class LargeMessageSerializer(Codec):
    def __init__(
        self,
        output_topic: str,
        config: LargeMessageSerializerConfig,
        is_key: bool = False,
        **kwargs
    ):
        super().__init__(
            output_topic=output_topic, config=config, is_key=is_key, **kwargs
        )
        self._output_topic = output_topic
        self._config = config
        self._is_key = is_key
        self._blob_client = BlobStorageFactory(config).get_blob_storage_client()
        self._storage_client = StorageClient(config, self._blob_client)
        self._retriever_client = RetrieverClient(config, self._blob_client)

    def _loads(self, s: bytes) -> bytes:
        return self._retriever_client.retrieve_bytes(s)

    def _dumps(self, s: bytes) -> bytes:
        return self._storage_client.store_bytes(self._output_topic, s, self._is_key)
