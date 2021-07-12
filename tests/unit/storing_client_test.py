from unittest.mock import MagicMock

import pytest

from faust_large_message_serializer import LargeMessageSerializerConfig
from faust_large_message_serializer.blob_storage.blob_storage import BlobStorageClient
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


def test_max_size_storing_should_be_backed(monkeypatch, config_serializer):

    blob_client = MagicMock(specs=BlobStorageClient)

    storing_client = StoringClient(blob_client, config_serializer.base_path, 0)

    storing_client.store_bytes("test-serializer", b"Hello World", False)

    blob_client.put_object.assert_called_once()


def test_max_size_storing_should_not_be_backed(monkeypatch, config_serializer):

    blob_client = MagicMock()

    storing_client = StoringClient(
        config_serializer, config_serializer.base_path, 10000
    )

    storing_client.store_bytes("test-serializer", b"Hello World", False)

    blob_client.put_object.assert_not_called()


def test_base_path_should_defined_when_backed(monkeypatch, config_serializer):

    blob_client = MagicMock()

    storing_client = StoringClient(blob_client, None, 0)

    with pytest.raises(ValueError) as e:
        storing_client.store_bytes("test-serializer", b"Hello World", False)

    assert (
        str(e.value) == "Base path must not be null"
    ), "Base path should exists when backed procedure was called"

    blob_client.put_object.assert_not_called()
