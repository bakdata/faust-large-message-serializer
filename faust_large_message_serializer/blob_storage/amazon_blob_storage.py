from faust_large_message_serializer.blob_storage.blob_storage import BlobStorageClient


class S3UploadException(Exception):
    pass


class AmazonS3Client(BlobStorageClient):

    PROTOCOL = "s3"

    def __init__(self, s3_client):
        self._s3_client = s3_client

    def delete_all_objects(self, bucket: str, prefix: str) -> None:
        objects_response = self._s3_client.list_object_versions(
            Bucket=bucket, Prefix=prefix
        )
        key_objects = [
            {"Key": object_s3["Key"], "VersionId": object_s3["VersionId"]}
            for object_s3 in objects_response["Versions"]
        ]
        self._s3_client.delete_objects(Bucket=bucket, Delete={"Objects": key_objects})

    def put_object(self, data: bytes, bucket: str, key: str) -> str:
        response = self._s3_client.put_object(Bucket=bucket, Key=key, Body=data)
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            return f"{self.PROTOCOL}://{bucket}/{key}"
        else:
            raise S3UploadException(f"Error uploading blob to S3: {str(response)}")

    def get_object(self, bucket: str, key: str) -> bytes:
        object_metadata = self._s3_client.get_object(Bucket=bucket, Key=key)
        raw_object = object_metadata["Body"].read()
        return raw_object
