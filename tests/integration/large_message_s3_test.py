import boto3
import pytest
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from faust_large_message_serializer.config import (
    LargeMessageSerializerConfig,
    URIParser,
)
from faust_large_message_serializer.serializer import LargeMessageSerializer

pytest_plugins = ["docker_compose"]

bucket_name = "my-test-bucket"
base_path = f"s3://{bucket_name}"
fake_secret = "fake_secret"
fake_key = "fake_key"
endpoint_url = "http://127.0.0.1:4566"
region = "us-east-1"
config = LargeMessageSerializerConfig(
    base_path,
    0,
    fake_secret,
    fake_key,
    region,
    endpoint_url,
)

output_topic = "test-serializer"


@pytest.fixture(scope="module")
def wait_for_api(module_scoped_container_getter):
    """Wait for the api from my_api_service to become responsive"""
    request_session = requests.Session()
    retries = Retry(total=20, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    request_session.mount("http://", HTTPAdapter(max_retries=retries))

    service = list(
        filter(
            lambda x: x.container_port == "4566/tcp",
            module_scoped_container_getter.get("localstack").network_info,
        )
    )[0]
    hostname = service.hostname if service.hostname != "0.0.0.0" else "127.0.0.1"
    api_url = "http://%s:%s/" % (hostname, service.host_port)
    result = request_session.get(api_url).json()
    assert result["status"] == "running"
    return request_session, api_url


@pytest.fixture(scope="function")
def s3_bucket_name():
    s3_config = {
        "aws_secret_access_key": config.large_message_s3_secret_key,
        "aws_access_key_id": config.large_message_s3_access_key,
        "region_name": config.large_message_s3_region,
        "endpoint_url": config.large_message_s3_endpoint,
    }
    s3_client = boto3.resource("s3", **s3_config)
    bucket = s3_client.Bucket(bucket_name)
    bucket.create()
    yield bucket_name
    bucket.objects.all().delete()
    bucket.delete()


@pytest.fixture(scope="function")
def s3_low_level_client():
    s3_config = {
        "aws_secret_access_key": config.large_message_s3_secret_key,
        "aws_access_key_id": config.large_message_s3_access_key,
        "region_name": config.large_message_s3_region,
        "endpoint_url": config.large_message_s3_endpoint,
    }
    s3_client = boto3.resource("s3", **s3_config)
    yield s3_client


def test_s3_serializer_value(wait_for_api, s3_bucket_name):
    serializer = LargeMessageSerializer(output_topic, config)
    binary_input = b"This is a test for s3"
    s3_uri = serializer.dumps(binary_input)
    assert s3_uri[0:1] == b"\x01"
    parser_uri = URIParser(s3_uri[1:].decode())
    assert f"s3://{s3_bucket_name}/{output_topic}/values" in parser_uri.base_path
    assert binary_input == serializer.loads(s3_uri)


def test_s3_serializer_key(wait_for_api, s3_bucket_name):
    serializer = LargeMessageSerializer(output_topic, config, True)
    binary_input = b"This is a test for s3"
    s3_uri = serializer.dumps(binary_input)
    assert s3_uri[0:1] == b"\x01"
    parser_uri = URIParser(s3_uri[1:].decode())
    assert f"s3://{s3_bucket_name}/{output_topic}/keys" in parser_uri.base_path
    assert binary_input == serializer.loads(s3_uri)


def test_s3_storage_client_delete_all(wait_for_api, s3_low_level_client):
    bucket_name_delete = "test-delete-bucket"
    bucket = s3_low_level_client.Bucket(bucket_name_delete)
    bucket.create()
    s3_client = config.create_storing_client()._client
    s3_client.put_object(b"Test 1", bucket_name_delete, "foo/first_test.txt")
    s3_client.put_object(b"Test 2", bucket_name_delete, "foo/second_test.txt")
    all_objects = list(bucket.objects.all())
    assert len(all_objects) == 2, "Should put two objects inside foo"

    s3_client.put_object(b"Test 2", bucket_name_delete, "bar/third_test.txt")
    all_objects = list(bucket.objects.all())
    assert len(all_objects) == 3, "Should put one objects inside bar"

    s3_client.delete_all_objects(bucket_name_delete, "foo")
    all_objects = list(bucket.objects.all())
    assert len(all_objects) == 1, "Should delete all foo"

    s3_client.delete_all_objects(bucket_name_delete, "bar")
    all_objects = list(bucket.objects.all())
    assert len(all_objects) == 0, "Should delete all bar and be empty"

    bucket.delete()
