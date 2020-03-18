#  MIT License
#  Copyright (c) 2020 bakdata
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.

from io import BytesIO
from typing import Callable
from unittest.mock import patch
import pytest
import boto3
from botocore.stub import Stubber
from faust_s3_backed_serializer.s3_serializer import S3BackedSerializer, S3UploadException
from tests.utils import parse_uri_s3_serde


@pytest.fixture
def s3_client_put():
    client = boto3.client('s3')
    stubber = Stubber(client)
    mocked_response_put_object = {
        "ResponseMetadata": {
            "HTTPStatusCode": 200
        }
    }
    stubber.add_response("put_object", mocked_response_put_object)
    stubber.activate()
    return client


@pytest.fixture
def s3_client_put_error():
    client = boto3.client('s3')
    stubber = Stubber(client)
    mocked_response_put_object = {
        "ResponseMetadata": {
            "HTTPStatusCode": 404
        }
    }
    stubber.add_response("put_object", mocked_response_put_object)
    stubber.activate()
    return client


@pytest.fixture
def s3_client_get():
    client = boto3.client('s3')
    stubber = Stubber(client)
    dummy_body = BytesIO(b"dummy_string")
    mocked_response_get_object = {
        "Body": dummy_body
    }

    stubber.add_response("get_object", mocked_response_get_object)
    stubber.activate()
    return client


@pytest.fixture
def s3_serializer_factory() -> Callable[[int, bool], S3BackedSerializer]:
    def partial_factory(max_size: int, is_key: bool) -> S3BackedSerializer:
        credentials = {}
        return S3BackedSerializer("dummy_topic", "s3://bucket-name/", "eu-central-1", credentials, max_size, is_key)

    return partial_factory


def test_loads_from_s3_serde_min_size_value(s3_serializer_factory, s3_client_get):
    s3_serializer_min_size = s3_serializer_factory(0, False)
    with patch.object(s3_serializer_min_size, "_s3_client", s3_client_get):
        s3_fake_uri = b"\x01s3://bucket-name/dummy_topic/values/random_uuid"
        dummy_bytes = s3_serializer_min_size.loads(s3_fake_uri)
        assert dummy_bytes.decode(
            "utf-8") == "dummy_string", "The s3 serializer should give you the data from an s3 uri"


def test_dumps_from_s3_serde_min_size_value(s3_serializer_factory, s3_client_put):
    s3_serializer_min_size = s3_serializer_factory(0, False)
    with patch.object(s3_serializer_min_size, "_s3_client", s3_client_put):
        data_bytes = b"this is my data"
        s3_uri = s3_serializer_min_size.dumps(data_bytes)
        s3_uri_str = s3_uri[1:].decode("utf-8")
        bucket, path = parse_uri_s3_serde(s3_uri_str)
        split_path = path.split("/")
        assert s3_uri[0] == 1, "The signal byte should be 1"
        assert bucket == "bucket-name", "Bucket name should be in the uri"
        assert len(split_path) == 3, "The uri should have a depth of 3"
        assert split_path[0] == "dummy_topic", "The topic should be in the uri to signal the root folder"
        assert split_path[1] == "values", "Values should be in the uri to signal the values from a kafka record"
        assert isinstance(split_path[2], str), "The last random uuid should exist"


def test_dumps_from_s3_serde_min_size_value_error(s3_serializer_factory, s3_client_put_error):
    s3_serializer_min_size = s3_serializer_factory(0, False)
    with patch.object(s3_serializer_min_size, "_s3_client", s3_client_put_error):
        with pytest.raises(S3UploadException) as e:
            data_bytes = b"this is my data"
            s3_uri = s3_serializer_min_size.dumps(data_bytes)
        assert str(e.value) == "The S3 client couldn't upload the data to the s3 bucket"


def test_dumps_from_s3_serde_min_size_key(s3_serializer_factory, s3_client_put):
    s3_serializer_min_size = s3_serializer_factory(0, True)
    with patch.object(s3_serializer_min_size, "_s3_client", s3_client_put):
        data_bytes = b"this is my data"
        s3_uri = s3_serializer_min_size.dumps(data_bytes)
        s3_uri_str = s3_uri[1:].decode("utf-8")
        bucket, path = parse_uri_s3_serde(s3_uri_str)
        split_path = path.split("/")
        assert s3_uri[0] == 1, "The signal byte should be 1"
        assert bucket == "bucket-name", "Bucket name should be in the uri"
        assert len(split_path) == 3, "The uri should have a depth of 3"
        assert split_path[0] == "dummy_topic", "The topic should be in the uri to signal the root folder"
        assert split_path[1] == "keys", "Values should be in the uri to signal the values from a kafka record"
        assert isinstance(split_path[2], str)


def test_dumps_from_s3_serde_max_size_value(s3_serializer_factory):
    s3_serializer_max_size = s3_serializer_factory(100000, False)
    data_bytes = b"this is my data"
    s3_same_object = s3_serializer_max_size.dumps(data_bytes)
    assert s3_same_object[0] == 0, "The signal byte should be 0"
    assert s3_same_object[1:] == data_bytes, "The output should be the same without the signal byte"


def test_dumps_from_s3_serde_max_size_value(s3_serializer_factory):
    s3_serializer_max_size = s3_serializer_factory(100000, False)
    data_bytes = b"\x00this is my data"
    s3_same_object = s3_serializer_max_size.loads(data_bytes)
    assert data_bytes[1:] == s3_same_object, "The output should be the same without the signal byte"


def test_base_params():
    dummy_bytes = b"dummy-bytes"
    with pytest.raises(ValueError) as e:
        serializer = S3BackedSerializer(output_topic="output_topic", max_size=0)
        serializer.dumps(dummy_bytes)

    assert str(e.value) == "base_path should not be None"

    with pytest.raises(ValueError) as e:
        serializer = S3BackedSerializer(base_path="s3://something",max_size=0)
        serializer.dumps(dummy_bytes)

    assert str(e.value) == "output_topic should not be None"
