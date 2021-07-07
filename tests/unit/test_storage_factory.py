from unittest.mock import MagicMock

from faust_large_message_serializer import LargeMessageSerializerConfig
from faust_large_message_serializer.blob_storage.blog_storage_factory import (
    BlobStorageFactory,
)


def test_storage_factory_cache(monkeypatch):
    bucket_name = "my-test-bucket"
    base_path = f"s3://{bucket_name}"
    fake_secret = "fake_secret"
    fake_key = "fake_key"
    endpoint_url = "http://127.0.0.1:4566"
    region = "us-east-1"
    s3_client_mock = MagicMock(return_value=MagicMock())
    monkeypatch.setattr(
        "faust_large_message_serializer.blob_storage.blog_storage_factory.boto3.client",
        s3_client_mock,
    )
    config = LargeMessageSerializerConfig(
        base_path, 0, fake_secret, fake_key, region, endpoint_url
    )
    factory = BlobStorageFactory(config)
    assert (
        factory.get_blob_storage_client() == factory.get_blob_storage_client()
    ), "Client should be cached"
