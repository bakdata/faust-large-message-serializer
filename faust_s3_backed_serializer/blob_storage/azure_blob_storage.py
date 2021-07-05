from azure.storage.blob import BlobServiceClient

from faust_s3_backed_serializer.blob_storage.blob_storage import BlobStorageClient


class AzureBlobStorage(BlobStorageClient):
    def __init__(self, abs_service_client: BlobServiceClient):
        self._abs_client = abs_service_client

    def delete_all_objects(self, bucket: str, prefix: str) -> None:
        client_container = self._abs_client.get_container_client(bucket)
        blobs_container = client_container.list_blobs(name_starts_with=prefix)
        blob_names = [blob.name for blob in blobs_container]
        client_container.delete_blobs(*blob_names)

    def put_object(self, data: bytes, bucket: str, key: str) -> str:
        client_container = self._abs_client.get_container_client(bucket)
        blob_client = client_container.get_blob_client(key)
        blob_client.upload_blob(data)
        return f"abs://{bucket}/{key}"

    def get_object(self, bucket: str, key: str) -> bytes:
        client_container = self._abs_client.get_container_client(bucket)
        blob_client = client_container.get_blob_client(key)
        return blob_client.download_blob().readall()
