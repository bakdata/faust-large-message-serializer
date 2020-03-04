# faust-s3-backed-serializer
[![GitHub license](https://img.shields.io/github/license/bakdata/faust-s3-backed-serializer)](https://github.com/bakdata/faust-s3-backed-serializer/blob/master/LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.6%20%7C%203.7%20%7C%203.8-blue.svg)](https://img.shields.io/badge/python-3.6%20%7C%203.7-blue.svg)
[![Build Status](https://dev.azure.com/bakdata/public/_apis/build/status/bakdata.faust-s3-backed-serializer?branchName=master)](https://dev.azure.com/bakdata/public/_build/latest?definitionId=22&branchName=master)

# A Simple Example
```python
import faust
from faust import Record
import logging
from faust_s3_backed_serializer import S3BackedSerializer
from faust.serializers import codecs


# model.user
class UserModel(Record, serializer="s3_json"):
    first_name: str
    last_name: str


# Declare the serializers
credentials = {
    's3backed.access.key': 'access_key',
    's3backed.secret.key': 'secret_key'
}
s3_backed_serializer = S3BackedSerializer("s3_users", "s3://you-bucket-name/", "eu-central-1", credentials, 0, False)
json_serializer = codecs.get_codec("json")

# Concatenate them
s3_json_serializer = json_serializer | s3_backed_serializer

# config
logger = logging.getLogger(__name__)
codecs.register("s3_json", s3_json_serializer)
app = faust.App("app_id", broker="kafka://localhost:9092")
users_topic = app.topic('users_s3', value_type=UserModel)


@app.agent(users_topic)
async def users(users):
    async for user in users:
        logger.info("Event received in topic")
        logger.info(f"The user is : {user}")


@app.timer(5.0, on_leader=True)
async def send_users():
    data_user = {"first_name": "bar", "last_name": "foo"}
    user = UserModel(**data_user)
    await users.send(value=user, value_serializer=s3_json_serializer)


app.main()

````