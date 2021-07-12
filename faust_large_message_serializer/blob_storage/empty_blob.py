from faust_large_message_serializer.blob_storage.blob_storage import BlobStorageClient


class EmptyBlobStorage(BlobStorageClient):
    def delete_all_objects(self, bucket: str, prefix: str) -> None:
        raise NotImplementedError()

    def put_object(self, data: bytes, bucket: str, key: str) -> str:
        raise NotImplementedError()

    def get_object(self, bucket: str, key: str) -> bytes:
        raise NotImplementedError()
