from azure.storage.blob import BlobServiceClient

from faust_large_message_serializer.blob_storage.blob_storage import BlobStorageClient


class AzureBlobStorageClient(BlobStorageClient):

    PROTOCOL = "abs"

    def __init__(self, abs_service_client: BlobServiceClient):
        self._abs_client = abs_service_client

    def delete_all_objects(self, bucket: str, prefix: str) -> None:
        container_client = self._abs_client.get_container_client(bucket)
        blobs = container_client.list_blobs(name_starts_with=prefix)
        for blob in blobs:
            container_client.delete_blob(blob.name)

    def put_object(self, data: bytes, bucket: str, key: str) -> str:
        container_client = self._abs_client.get_container_client(bucket)
        blob_client = container_client.get_blob_client(key)
        blob_client.upload_blob(data)
        return f"{self.PROTOCOL}://{bucket}/{key}"

    def get_object(self, bucket: str, key: str) -> bytes:
        container_client = self._abs_client.get_container_client(bucket)
        blob_client = container_client.get_blob_client(key)
        return blob_client.download_blob().readall()
