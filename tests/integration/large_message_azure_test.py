import pytest
import requests
from azure.storage.blob import BlobServiceClient
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from faust_large_message_serializer.config import (
    LargeMessageSerializerConfig,
    URIParser,
)
from faust_large_message_serializer.serializer import LargeMessageSerializer

pytest_plugins = ["docker_compose"]

bucket_name = "abs-bucket"
base_path = f"abs://{bucket_name}"
fake_secret = "fake_secret"
fake_key = "fake_key"
endpoint_url = "http://127.0.0.1:10000"
region = "us-east-1"
config = LargeMessageSerializerConfig(
    base_path,
    0,
    large_message_abs_connection_string=f"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint={endpoint_url}/devstoreaccount1;",
)

output_topic = "test-serializer"


@pytest.fixture(scope="module")
def wait_for_api(module_scoped_container_getter):
    request_session = requests.Session()
    retries = Retry(total=20, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    request_session.mount("http://", HTTPAdapter(max_retries=retries))

    service = list(
        filter(
            lambda x: x.container_port == "10000/tcp",
            module_scoped_container_getter.get("azurite").network_info,
        )
    )[0]
    hostname = service.hostname if service.hostname != "0.0.0.0" else "127.0.0.1"
    api_url = "http://%s:%s/" % (hostname, service.host_port)
    result = request_session.get(api_url)
    assert result.status_code == 400
    return request_session, api_url


@pytest.fixture(scope="function")
def azure_bucket_name():
    abs_client = BlobServiceClient.from_connection_string(
        config.large_message_abs_connection_string
    )
    container_client = abs_client.get_container_client(bucket_name)
    container_client.create_container()
    yield bucket_name
    container_client.delete_container()


def test_azure_serializer_value(wait_for_api, azure_bucket_name):
    serializer = LargeMessageSerializer(output_topic, config)
    binary_input = b"This is a test for s3"
    abs_uri = serializer.dumps(binary_input)
    assert abs_uri[0:1] == b"\x01", "Uri should be backed"
    parser_uri = URIParser(abs_uri[1:].decode())
    assert f"abs://{azure_bucket_name}/{output_topic}/values" in parser_uri.base_path, "Schema and key-value var should correspond"
    assert binary_input == serializer.loads(abs_uri), "URI should be unbacked"


def test_azure_serializer_key(wait_for_api, azure_bucket_name):
    serializer = LargeMessageSerializer(output_topic, config, True)
    binary_input = b"This is a test for s3"
    abs_uri = serializer.dumps(binary_input)
    assert abs_uri[0:1] == b"\x01", "Uri should be backed"
    parser_uri = URIParser(abs_uri[1:].decode())
    assert f"abs://{azure_bucket_name}/{output_topic}/keys" in parser_uri.base_path, "Schema and key-value var should correspond"
    assert binary_input == serializer.loads(abs_uri), "URI should be unbacked"


@pytest.fixture(scope="function")
def abs_low_level_client():
    abs_client = BlobServiceClient.from_connection_string(config.large_message_abs_connection_string)
    yield abs_client


def test_azure_client_delete_all(abs_low_level_client):
    bucket_name_delete = "test-delete-bucket"
    container_client = abs_low_level_client.get_container_client(bucket_name_delete)
    container_client.create_container()

    abs_client = config.create_storing_client()._client
    abs_client.put_object(b"Test 1", bucket_name_delete, "foo/first_test.txt")
    abs_client.put_object(b"Test 2", bucket_name_delete, "foo/second_test.txt")
    all_objects = list(container_client.list_blobs())
    assert len(all_objects) == 2, "Should put two objects inside foo"

    abs_client.put_object(b"Test 2", bucket_name_delete, "bar/third_test.txt")
    all_objects = list(container_client.list_blobs())
    assert len(all_objects) == 3, "Should put one objects inside bar"

    abs_client.delete_all_objects(bucket_name_delete, "foo")
    all_objects = list(container_client.list_blobs())
    assert len(all_objects) == 1, "Should delete all foo"

    abs_client.delete_all_objects(bucket_name_delete, "bar")
    all_objects = list(container_client.list_blobs())
    assert len(all_objects) == 0, "Should delete all bar and be empty"