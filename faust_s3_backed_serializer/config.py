from dataclasses import InitVar
from typing import Optional, Callable, Dict, Tuple, Union
from urllib.parse import urlparse

from attr import dataclass


@dataclass
class URIParser:
    base_path: str

    def __post_init__(self):
        self.base_path = self.base_path.lower()

    def parse_uri(self) -> Tuple[str, str, str]:
        result = urlparse(self.base_path)
        scheme = result.scheme
        bucket = result.netloc
        if result.path[0] == "/":
            path = result.path[1:]
        else:
            path = result.path
        return scheme, bucket, path


    def __repr__(self):
        return self.base_path


@dataclass
class LargeMessageSerializerConfig:
    base_path: Union[str, URIParser]
    max_size: int
    output_topic: str
    large_message_s3_secret_key: Optional[str]
    large_message_s3_access_key: Optional[str]
    large_message_s3_region: Optional[str]
    large_message_s3_endpoint: Optional[str]
    large_message_s3_role_external_id: Optional[str]
    large_message_abs_connection_string: Optional[str]
    large_message_blob_storage_custom_config: Optional[
        Callable[[Dict[str, Optional[str]]], None]
    ]

    def __post_init__(self):
        if not isinstance(self.base_path, str) and not isinstance(self.base_path, URIParser):
            raise ValueError("base_path should be an str or a URIParser")
        if self.base_path is None:
            raise ValueError("You need to give a base_path")
        self.base_path = (
            URIParser(self.base_path) if isinstance(self.base_path, str) else base_path
        )
