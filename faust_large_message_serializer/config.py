from typing import Optional, Callable, Dict, Tuple, Union
from urllib.parse import urlparse

from dataclasses import dataclass


@dataclass
class URIParser:
    base_path: str

    def __post_init__(self):
        self.base_path = self.base_path.lower()

    def parse_uri(self) -> Tuple[str, str, str]:
        result = urlparse(self.base_path)
        scheme = result.scheme
        bucket = result.netloc
        if len(result.path) != 0 and result.path[0] == "/":
            path = result.path[1:]
        else:
            path = result.path
        return scheme, bucket, path

    def __repr__(self):
        return self.base_path


@dataclass
class LargeMessageSerializerConfig:
    base_path: Optional[Union[str, URIParser]] = None
    max_size: int = 1000 * 1000
    large_message_s3_secret_key: Optional[str] = None
    large_message_s3_access_key: Optional[str] = None
    large_message_s3_region: Optional[str] = None
    large_message_s3_endpoint: Optional[str] = None
    large_message_abs_connection_string: Optional[str] = None
    large_message_blob_storage_custom_config: Optional[
        Callable[[Dict[str, Optional[str]]], None]
    ] = None

    def __post_init__(self):
        self.base_path = (
            URIParser(self.base_path)
            if isinstance(self.base_path, str)
            else self.base_path
        )