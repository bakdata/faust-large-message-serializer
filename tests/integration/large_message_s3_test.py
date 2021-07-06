import pytest
import requests
from urllib.parse import urljoin
import boto3
from faust_large_message_serializer.serializer import LargeMessageSerializer

from faust_large_message_serializer.config import LargeMessageSerializerConfig, URIParser
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

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
serializer = LargeMessageSerializer(output_topic, config)

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


def test_s3_serializer(wait_for_api, s3_bucket_name):
    """The Api is now verified good to go and tests can interact with it"""
    binary_input = b"This is a test for s3"
    s3_uri = serializer.dumps(binary_input)
    assert s3_uri[0:1] == b"\x01"
    parser_uri = URIParser(s3_uri[1:].decode())
    assert f"s3://{s3_bucket_name}/{output_topic}/values" in parser_uri.base_path
    assert binary_input == serializer.loads(s3_uri)
