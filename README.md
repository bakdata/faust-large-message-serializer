[![GitHub license](https://img.shields.io/github/license/bakdata/faust-s3-backed-serializer)](https://github.com/bakdata/faust-large-message-serializer/blob/master/LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.6%20%7C%203.7%20%7C%203.8-blue.svg)](https://img.shields.io/badge/python-3.6%20%7C%203.7-blue.svg)
[![Build Status](https://dev.azure.com/bakdata/public/_apis/build/status/bakdata.faust-large-message-serializer?branchName=master)](https://dev.azure.com/bakdata/public/_build/latest?definitionId=22&branchName=master)
[![PyPI version](https://badge.fury.io/py/faust-large-message-serializer.svg)](https://badge.fury.io/py/faust-large-message-serializer)
# faust-large-message-serializer

A Faust Serializer that reads and writes records from and to S3 or Azure Blob Storage transparently.

This serializer is compatible with our [Kafka large-message-serializer SerDe](https://github.com/bakdata/kafka-large-message-serde) for Java.

Read more about it on our [blog](https://medium.com/bakdata/processing-large-messages-with-kafka-streams-167a166ca38b).

# Getting Started

#### PyPi

```
pip install faust-large-message-serializer
```


##### Usage

The serializer was build to be used with other serializers. The idea is to use the ["concatenation" feature](https://faust.readthedocs.io/en/latest/userguide/models.html#codec-registry) that comes with Faust

```python
import faust
from faust import Record
import logging
from faust_large_message_serializer import LargeMessageSerializer, LargeMessageSerializerConfig
from faust.serializers import codecs


# model.user
class UserModel(Record, serializer="s3_json"):
    first_name: str
    last_name: str


config = LargeMessageSerializerConfig(base_path="s3://your-bucket-name/",
                                      max_size=0,
                                      large_message_s3_region="eu-central-1",
                                      large_message_s3_access_key="access_key",
                                      large_message_s3_secret_key="secret_key")

topic_name = "users_s3"
s3_backed_serializer = LargeMessageSerializer(topic_name, config, is_key=False)
json_serializer = codecs.get_codec("json")

# Here we use json as the first serializer and
# then we can upload everything to the S3 bucket
s3_json_serializer = json_serializer | s3_backed_serializer

# config
logger = logging.getLogger(__name__)
codecs.register("s3_json", s3_json_serializer)
app = faust.App("app_id", broker="kafka://localhost:9092")
users_topic = app.topic(topic_name, value_type=UserModel)


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


## Contributing

We are happy if you want to contribute to this project.
If you find any bugs or have suggestions for improvements, please open an issue.
We are also happy to accept your PRs.
Just open an issue beforehand and let us know what you want to do and why.

## License
This project is licensed under the MIT license.
Have a look at the [LICENSE](https://github.com/bakdata/faust-s3-backed-serializer/blob/master/LICENSE) for more details.