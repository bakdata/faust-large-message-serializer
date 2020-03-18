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

from typing import Any, Tuple, Optional
import boto3
from faust import Codec
from urllib.parse import urlparse
import uuid
from typing import Dict


class S3UploadException(Exception):
    pass


class S3BackedSerializer(Codec):
    """Faust serializer that serializes large messages on Amazon S3.

    If the message size exceeds a defined threshold, the payload is uploaded to Amazon S3.
    The message forwarded to Kafka contains a flag if the message has been backed or not.
    In case it was backed, the flag is followed by the URI of the S3 object.
    If the message was not backed, it contains the actual serialized message.

    """

    VALUE_PREFIX = "values"
    KEY_PREFIX = "keys"

    def __init__(self, output_topic: Optional[str] = None, base_path: Optional[str] = None,
                 region_name: Optional[str] = None,
                 s3_credentials: Optional[Dict[str, str]] = None,
                 max_size: int = int(1e6),
                 is_key: bool = False,
                 **kwargs):
        """
        Parameters
        ----------
        output_topic : Optional[str]
            Topic where the data is sent to. Is used as part of the S3 object's name
        base_path: Optional[str]
            Base path to store data. Must include bucket and any prefix that should be used, e.g.,
            's3://my-bucket/my/prefix/'.
        region: str
            S3 region to use. Must be configured in conjunction. e.g., 'eu-central-1'
        s3_credentials: Optional[Dict[str, str]]
            AWS secret key to use for connecting to S3. Leave empty if AWS credential provider chain should be used.
            The s3_credentials dictionary should have the following properties::
                {
                    's3backed.access.key': 'my_s3_user_key',
                    's3backed.secret.key': 'my_s3_secret_key'
                }
        max_size: int
            Maximum serialized message size in bytes before messages are stored on S3.
        is_key: bool
            Variable to indicate the serializer to serialize a key or a value.
        **kwargs: Dict
            Extra keyword arguments.

        Returns
        -------
        S3BackedSerializer
            Faust serializer that serializes large messages on Amazon S3

        Raises
        ------
        ValueError
            If 'base_path' param is empty or not given when dumps is called and message is too large
            If 'output_topic' params is empty or not given when dumps is called and message is too large

        """

        super().__init__(output_topic=output_topic, base_path=base_path, max_size=max_size,
                         s3_credentials=s3_credentials, region_name=region_name,
                         **kwargs)

        self._base_path = base_path
        self._max_size = max_size
        self._is_key = is_key
        self._output_topic = output_topic

        self._credentials = self._build_s3_credentials(s3_credentials)
        self._s3_config = dict(self._credentials)
        self._s3_config.update(region_name=region_name)
        self._s3_bucket = self._parse_bucket_from_base_path(base_path)
        self._s3_client = boto3.client("s3", **self._s3_config)

    def _parse_bucket_from_base_path(self, base_path: str) -> str:
        parsed_url = urlparse(base_path)
        return parsed_url.netloc

    def _loads(self, data: bytes) -> Any:

        if self._should_download(data[0]):
            uri = data[1:].decode("utf-8")
            bucket, key = self._parse_uri_s3_serde(uri)
            object_metadata = self._s3_client.get_object(Bucket=bucket, Key=key)
            raw_object = object_metadata["Body"].read()
            return raw_object
        else:
            data_without_signal_byte = data[1:]
            return data_without_signal_byte

    def _parse_uri_s3_serde(self, uri: str) -> Tuple[str, str]:
        result = urlparse(uri)
        bucket = result.netloc
        if result.path[0] == "/":
            path = result.path[1:]
        else:
            path = result.path
        return bucket, path

    def _dumps(self, data: Any) -> bytes:
        length_obj = len(data)
        if length_obj > self._max_size:
            self._verify_params()
            key = self._generate_key(self._output_topic, self._is_key)
            response = self._s3_client.put_object(Bucket=self._s3_bucket, Key=key, Body=data)
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                signal_byte = b"\x01"
                s3_uri = f"s3://{self._s3_bucket}/{key}"
                s3_uri_encoded = signal_byte + s3_uri.encode("utf-8")
                return s3_uri_encoded
            else:
                raise S3UploadException("The S3 client couldn't upload the data to the s3 bucket")
        else:
            signal_byte = b"\x00"
            data_with_signal_byte = signal_byte + data
            return data_with_signal_byte

    def _generate_key(self, output_topic: str, is_key) -> str:
        key = f"{output_topic}/{self.VALUE_PREFIX if is_key == False else self.KEY_PREFIX}/{uuid.uuid4()}"
        return key

    def _should_download(self, protocol_byte: int) -> bool:
        return protocol_byte == 1

    def _build_s3_credentials(self, config: Dict[str, str]) -> Dict[str, str]:
        credentials = {}

        if not config or not config.get("s3backed.access.key") or not config.get("s3backed.secret.key"):
            return credentials

        credentials.update(aws_access_key_id=config["s3backed.access.key"])
        credentials.update(aws_secret_access_key=config["s3backed.secret.key"])

        return credentials

    def _verify_params(self):
        if self._base_path is None:
            raise ValueError("base_path should not be None")
        if self._output_topic is None:
            raise ValueError("output_topic should not be None")
