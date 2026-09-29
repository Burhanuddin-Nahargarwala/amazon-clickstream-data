import json
import time
from decimal import Decimal

import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from constant import ACTIVITY_TABLE_NAME, ACTIVITY_TTL_SECONDS, MAX_STREAM_SECONDS, STALE_JOB_SECONDS

# Each job has one summary item (seq 0) followed by one item per sent event (seq 1, 2, 3, ...)
SUMMARY_SEQ = 0

_serializer = TypeSerializer()
_deserializer = TypeDeserializer()


def make_job_id(email_id, called_at):
    return f"{email_id}#{called_at}"


def _to_dynamo(value):
    # DynamoDB rejects Python floats, so store them as Decimal
    if isinstance(value, float):
        value = Decimal(str(value))
    return _serializer.serialize(value)


def _from_dynamo(item):
    result = {}
    for key, value in item.items():
        value = _deserializer.deserialize(value)
        if isinstance(value, Decimal):
            value = int(value) if value == value.to_integral_value() else float(value)
        result[key] = value
    return result


class JobActivity:
    """Reads and writes live job progress in the activity table."""

    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name, table_name: str = ACTIVITY_TABLE_NAME):
        self.table_name = table_name

        self.dynamodb_client = boto3.client(
            service_name="dynamodb",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

    def _key(self, job_id, seq=SUMMARY_SEQ):
        return {"job_id": {"S": job_id}, "seq": {"N": str(seq)}}

    def create_summary(self, job_id, email_id, cloud_platform, target):
        now = time.time()
        item = {
            "job_id": job_id,
            "seq": SUMMARY_SEQ,
            "email_id": email_id,
            "cloud_platform": cloud_platform,
            "target": target,
            "status": "in_progress",
            "started_at": now,
            "last_event_at": now,
            "events_sent": 0,
            "stop_requested": False,
            "max_duration_seconds": MAX_STREAM_SECONDS,
            "expires_at": int(now) + ACTIVITY_TTL_SECONDS,
        }
        self.dynamodb_client.put_item(
            TableName=self.table_name,
            Item={key: _to_dynamo(value) for key, value in item.items()},
        )

    def update_summary(self, job_id, **fields):
        names = {f"#{key}": key for key in fields}
        values = {f":{key}": _to_dynamo(value) for key, value in fields.items()}
        self.dynamodb_client.update_item(
            TableName=self.table_name,
            Key=self._key(job_id),
            UpdateExpression="SET " + ", ".join(f"#{key} = :{key}" for key in fields),
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
        )

    def get_summary(self, job_id):
        response = self.dynamodb_client.get_item(
            TableName=self.table_name, Key=self._key(job_id), ConsistentRead=True
        )
        item = response.get("Item")
        return _from_dynamo(item) if item else None

    def record_event(self, job_id, seq, event, batch):
        now = time.time()
        item = {
            "job_id": job_id,
            "seq": seq,
            "sent_at": now,
            "batch": batch,
            "event_type": event.get("event_type", ""),
            "session_id": event.get("session_id", ""),
            "user_id": event.get("user_id", ""),
            "payload": json.dumps(event),
            "expires_at": int(now) + ACTIVITY_TTL_SECONDS,
        }
        self.dynamodb_client.put_item(
            TableName=self.table_name,
            Item={key: _to_dynamo(value) for key, value in item.items()},
        )
        self.update_summary(job_id, events_sent=seq, last_event_at=now)

    def get_events(self, job_id, after: int = 0, limit: int = 100):
        response = self.dynamodb_client.query(
            TableName=self.table_name,
            KeyConditionExpression="job_id = :job_id AND seq > :after",
            ExpressionAttributeValues={
                ":job_id": {"S": job_id},
                ":after": {"N": str(max(after, SUMMARY_SEQ))},
            },
            ScanIndexForward=True,
            Limit=limit,
        )
        return [_from_dynamo(item) for item in response.get("Items", [])]

    def is_stale(self, summary):
        return time.time() - summary.get("last_event_at", 0) > STALE_JOB_SECONDS


class JobTracker:
    """Per-run helper used by the data generation loop: numbers events, records failures, checks for stop."""

    STOP_CHECK_INTERVAL_SECONDS = 3

    def __init__(self, activity: JobActivity, dynamodb, email_id, called_at):
        self.activity = activity
        self.dynamodb = dynamodb
        self.email_id = email_id
        self.called_at = called_at
        self.job_id = make_job_id(email_id, called_at)
        self.seq = 0
        self.failed = False
        self.started_at = time.time()
        self.max_duration_seconds = MAX_STREAM_SECONDS
        self._stop_requested = False
        self._last_stop_check = 0.0

    def record_event(self, event, batch):
        # Runs on a single asyncio loop, so incrementing the counter here is safe across batches
        self.seq += 1
        try:
            self.activity.record_event(self.job_id, self.seq, event, batch)
        except Exception as err:
            # The event already reached the user's stream; a missed log line must not stop the job
            print(f"Could not record event {self.seq} for {self.job_id}: {err}")

    def stop_requested(self):
        if self._stop_requested:
            return True
        now = time.time()
        if now - self._last_stop_check >= self.STOP_CHECK_INTERVAL_SECONDS:
            self._last_stop_check = now
            try:
                summary = self.activity.get_summary(self.job_id)
                self._stop_requested = bool(summary and summary.get("stop_requested"))
            except Exception as err:
                print(f"Could not check stop flag for {self.job_id}: {err}")
        return self._stop_requested

    def time_limit_reached(self):
        return time.time() - self.started_at >= self.max_duration_seconds

    def finish(self, status, **extra_fields):
        self.activity.update_summary(self.job_id, status=status, finished_at=time.time(), **extra_fields)
        self.dynamodb.update_data(email_id=self.email_id, called_at=self.called_at, status_value=status)

    def fail(self, stream_error):
        # Several batches can fail at once; only the first error is reported
        if self.failed:
            return
        self.failed = True
        self.activity.update_summary(
            self.job_id,
            status="failed",
            finished_at=time.time(),
            error_message=stream_error.message,
            error_source=stream_error.source,
            error_detail=stream_error.detail,
        )
        self.dynamodb.update_data(email_id=self.email_id, called_at=self.called_at, status_value="failed")
