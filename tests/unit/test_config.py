from unittest.mock import MagicMock

from faust_large_message_serializer.config import LargeMessageSerializerConfig


def test_config_callback(monkeypatch):
    bucket_name = "my-test-bucket"
    base_path = f"s3://{bucket_name}"
    fake_secret = "fake_secret"
    fake_key = "fake_key"
    endpoint_url = "http://127.0.0.1:4566"
    region = "us-east-1"
    s3_client_mock = MagicMock()
    config_callback = MagicMock()
    config = LargeMessageSerializerConfig(
        base_path,
        0,
        fake_secret,
        fake_key,
        region,
        endpoint_url,
        large_message_blob_storage_custom_config=config_callback,
    )
    monkeypatch.setattr(
        "faust_large_message_serializer.config.boto3.client",
        s3_client_mock,
    )
    config.create_storing_client()
    s3_client_mock.assert_called_once()
    config_callback.assert_called_once()


def test_blob_client_cache(monkeypatch):
    bucket_name = "my-test-bucket"
    base_path = f"s3://{bucket_name}"
    fake_secret = "fake_secret"
    fake_key = "fake_key"
    endpoint_url = "http://127.0.0.1:4566"
    region = "us-east-1"
    s3_client_mock = MagicMock(return_value=MagicMock())
    monkeypatch.setattr(
        "faust_large_message_serializer.config.boto3.client",
        s3_client_mock,
    )
    config = LargeMessageSerializerConfig(
        base_path, 0, fake_secret, fake_key, region, endpoint_url
    )
    assert (
        config.create_storing_client()._client == config.create_storing_client()._client
    ), "Client should be cached"

    assert (
        config.create_retrieving_client()._client
        == config.create_retrieving_client()._client
    ), "Client should be cached"
