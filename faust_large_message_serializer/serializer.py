from faust.serializers.codecs import Codec

from faust_large_message_serializer.blob_storage.blog_storage_factory import (
    BlobStorageFactory,
)
from faust_large_message_serializer.clients.retriever_client import RetrievingClient
from faust_large_message_serializer.clients.storage_client import StoringClient
from faust_large_message_serializer.config import LargeMessageSerializerConfig


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
        self._blob_factory = BlobStorageFactory(config)
        self._storage_client = StoringClient(config, self._blob_factory)
        self._retriever_client = RetrievingClient(config, self._blob_factory)

    def _loads(self, s: bytes) -> bytes:
        return self._retriever_client.retrieve_bytes(s)

    def _dumps(self, s: bytes) -> bytes:
        return self._storage_client.store_bytes(self._output_topic, s, self._is_key)
