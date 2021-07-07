from unittest.mock import MagicMock

import pytest

from faust_large_message_serializer import LargeMessageSerializerConfig
from faust_large_message_serializer.blob_storage.blog_storage_factory import (
    BlobStorageFactory,
)
from faust_large_message_serializer.clients.storing_client import StoringClient


@pytest.fixture(scope="function")
def config_serializer():
    bucket_name = "my-test-bucket"
    base_path = f"s3://{bucket_name}"
    fake_secret = "fake_secret"
    fake_key = "fake_key"
    endpoint_url = "http://127.0.0.1:4566"
    region = "us-east-1"

    return LargeMessageSerializerConfig(
        base_path, 0, fake_secret, fake_key, region, endpoint_url
    )


def test_max_size_storing_should_not_backed(monkeypatch, config_serializer):

    s3_client_mock = MagicMock()
    blob_client = MagicMock()

    monkeypatch.setattr(
        "faust_large_message_serializer.blob_storage.blog_storage_factory.boto3.client",
        s3_client_mock,
    )

    serializer_factory = BlobStorageFactory(config_serializer)

    monkeypatch.setattr(serializer_factory, "get_blob_storage_client", blob_client)

    storing_client = StoringClient(config_serializer, serializer_factory)

    storing_client.store_bytes("test-serializer", b"Hello World", False)

    blob_client.assert_called_once()


def test_max_size_storing_should_backed(monkeypatch, config_serializer):

    s3_client_mock = MagicMock()
    blob_client = MagicMock()

    config_serializer.max_size = 100000

    monkeypatch.setattr(
        "faust_large_message_serializer.blob_storage.blog_storage_factory.boto3.client",
        s3_client_mock,
    )

    serializer_factory = BlobStorageFactory(config_serializer)

    monkeypatch.setattr(serializer_factory, "get_blob_storage_client", blob_client)

    storing_client = StoringClient(config_serializer, serializer_factory)

    storing_client.store_bytes("test-serializer", b"Hello World", False)

    blob_client.assert_not_called()


def test_base_path_should_defined_when_backed(monkeypatch, config_serializer):

    s3_client_mock = MagicMock()
    blob_client = MagicMock()

    config_serializer.max_size = 0
    config_serializer.base_path = None

    monkeypatch.setattr(
        "faust_large_message_serializer.blob_storage.blog_storage_factory.boto3.client",
        s3_client_mock,
    )

    serializer_factory = BlobStorageFactory(config_serializer)

    storing_client = StoringClient(config_serializer, serializer_factory)

    with pytest.raises(ValueError) as e:
        storing_client.store_bytes("test-serializer", b"Hello World", False)

    assert (
        str(e.value) == "Base path must not be null"
    ), "Base path should exists when backed procedure was called"

    blob_client.assert_not_called()
