from typing import Optional, Callable, Dict

from attr import dataclass


@dataclass
class LargeMessageSerializerConfig:
    base_path: str
    max_size: int
    output_topic: str
    large_message_s3_secret_key: Optional[str]
    large_message_s3_access_key: Optional[str]
    large_message_s3_region: Optional[str]
    large_message_s3_endpoint: Optional[str]
    large_message_s3_role_external_id: Optional[str]
    large_message_abs_connection_string: Optional[str]
    large_message_blob_storage_custom_config: Optional[Callable[[Dict[str, Optional[str]]], None]]

    def __post_init__(self):
        self.base_path = self.base_path.lower()
