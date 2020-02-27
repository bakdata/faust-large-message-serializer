from typing import Any, Tuple
import boto3
from faust import Codec
from urllib.parse import urlparse
import uuid
from typing import Dict


class S3UploadException(Exception):
    pass


class S3BackedSerializer(Codec):
    VALUE_PREFIX = "values"
    KEY_PREFIX = "keys"

    def __init__(self, output_topic: str, base_path: str, region_name: str, s3_credentials: Dict[str, str],
                 max_size: int = int(1e6),
                 is_key=False,
                 **kwargs):
        """
        Parameters
        ----------
        output_topic : str
            Topic where the data is sent to. Is used as part of the S3 object's name
        base_path: str
            S3 bucket name with the 's3://' uri protocol. Example 's3://bucket-name'
        region: str
            S3 region where the bucket is. Example 'eu-central-1'
        s3_credentials: Dict[str, str]
            The s3_credentials dictionary should have the following properties::
                {
                    's3backed.access.key': 'my_s3_user_key',
                    's3backed.secret.key': 'my_s3_secret_key'
                }
        max_size: int
            The limit of bytes that the serializer use to know when it should upload data to the s3 bucket.
        is_key: bool
            Variable to indicate serializer to serialize a key or a value.
        **kwargs: Dict
            Extra keyword arguments.

        Returns
        -------
        FaustS3Serializer
            Serializer to upload object to a bucket

        Raises
        ------
        ValueError
            If 'base_path' param is empty or not given
            If 'region_name' param is empty or not given
        """

        super().__init__(output_topic=output_topic, base_path=base_path, max_size=max_size,
                         s3_credentials=s3_credentials, region_name=region_name,
                         **kwargs)

        self._credentials = self._build_s3_credentials(s3_credentials)
        self._s3_config = dict(self._credentials)
        self._s3_config.update(region_name=region_name)
        self._output_topic = output_topic
        self._s3_bucket = self._parse_bucket_from_base_path(base_path)
        self._s3_client = boto3.client("s3", **self._s3_config)
        self._max_size = max_size
        self._is_key = is_key

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

        if not config.get("s3backed.access.key") or not config.get("s3backed.secret.key"):
            return credentials

        credentials.update(aws_access_key_id=config["s3backed.access.key"])
        credentials.update(aws_secret_access_key=config["s3backed.secret.key"])

        return credentials
