from abc import ABC, abstractmethod


class BlobStorageClient(ABC):

    @abstractmethod
    def delete_all_objects(self, bucket: str, prefix: str) -> None: ...
    @abstractmethod
    def put_object(self, data: bytes, bucket: str, key: str) -> str: ...
    @abstractmethod
    def get_object(self, bucket: str, key: str) -> bytes: ...
