"""
Shared AWS helpers, created once per worker process and reused.

Creating a boto3 client costs noticeable CPU and memory, and the console polls every 2 seconds per viewer,
so creating clients per request made memory climb under load. boto3 clients are safe to share between
threads once created; creating them is not, hence the lock.
"""
import threading

from constant import AWS_ACCESS_KEY_ID, AWS_REGION, AWS_SECRET_ACCESS_KEY, DYNAMODB_REGION
from dynamod_db import DynamoDB
from job_activity import JobActivity
from s3 import S3

_lock = threading.Lock()
_instances = {}


def _shared(name, factory):
    instance = _instances.get(name)
    if instance is None:
        with _lock:
            instance = _instances.get(name)
            if instance is None:
                instance = factory()
                _instances[name] = instance
    return instance


def get_dynamodb() -> DynamoDB:
    return _shared("dynamodb", lambda: DynamoDB(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, DYNAMODB_REGION))


def get_activity() -> JobActivity:
    return _shared("activity", lambda: JobActivity(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, DYNAMODB_REGION))


def get_s3() -> S3:
    return _shared("s3", lambda: S3(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    ))
