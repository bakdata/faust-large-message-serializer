from typing import Tuple
from urllib.parse import urlparse


def parse_uri_s3_serde(uri: str) -> Tuple[str, str]:
    result = urlparse(uri)
    bucket = result.netloc
    if result.path[0] == "/":
        path = result.path[1:]
    else:
        path = result.path
    return bucket, path
