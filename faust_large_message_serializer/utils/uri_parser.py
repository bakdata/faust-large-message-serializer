from dataclasses import dataclass
from typing import Tuple
from urllib.parse import urlparse


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
