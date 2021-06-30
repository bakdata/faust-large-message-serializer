from azure.storage.blob import BlobServiceClient

from faust_s3_backed_serializer.blob_storage.blob_storage import BlobStorageClient


class AzureBlobStorage(BlobStorageClient):

    def __init__(self, abs_service_client: BlobServiceClient):
        self._abs_client = abs_service_client


    def delete_all_objects(self, bucket: str, prefix: str) -> None:
        pass

    def put_object(self, data: bytes, bucket: str, key: str) -> str:
        ...

    def get_object(self, bucket: str, key: str) -> bytes:
        pass