from constant import BUCKET_NAME
import json
import random
import time
from google.cloud import pubsub_v1

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
import asyncio

from job_activity import JobTracker
from stream_errors import SERVER, StreamError, describe_azure_error, describe_gcp_error

# Session files are spread round-robin across this many concurrent batches
NUM_BATCHES = 5


class AzureSender:
    """
    Sends events to one Azure event hub over a single connection that is opened once per stream and shared
    by all batches (they run on the same asyncio loop). Opening a connection per event was the main CPU cost
    when many streams run at once.
    """

    def __init__(self, hub_name, connection_string):
        self.hub_name = hub_name
        self.producer = EventHubProducerClient.from_connection_string(
            connection_string, eventhub_name=hub_name
        )

    async def send(self, data_dict):
        try:
            await self.producer.send_batch([EventData(json.dumps(data_dict))])
            print(f"Event sent to Azure Event Hub: {data_dict}")
        except Exception as err:
            raise describe_azure_error(err, self.hub_name) from err

    async def close(self):
        await self.producer.close()


class GCPSender:
    """Publishes events to one Google Pub/Sub topic with a single publisher client per stream."""

    def __init__(self, credentials, project_id, topic_id):
        self.project_id = project_id
        self.topic_id = topic_id
        try:
            self.publisher = pubsub_v1.PublisherClient.from_service_account_info(credentials)
            self.topic_path = self.publisher.topic_path(project_id, topic_id)
        except Exception as err:
            raise describe_gcp_error(err, project_id, topic_id) from err

    async def send(self, data_dict):
        try:
            data_encoded_str = json.dumps(data_dict).encode("utf-8")
            future = self.publisher.publish(self.topic_path, data_encoded_str)
            # Await without blocking the loop, so the other batches keep sending meanwhile
            message_id = await asyncio.wrap_future(future)
            print(f"Published {data_encoded_str} to {self.topic_path}: {message_id}")
        except Exception as err:
            raise describe_gcp_error(err, self.project_id, self.topic_id) from err

    async def close(self):
        await asyncio.to_thread(self.publisher.stop)


def create_sender(cloud_platform, cloud_parameters):
    if cloud_platform == "Azure":
        return AzureSender(cloud_parameters.get("hub_name"), cloud_parameters.get("connection_string"))
    return GCPSender(
        cloud_parameters.get("credentials"),
        cloud_parameters.get("project_id"),
        cloud_parameters.get("topic_id"),
    )


async def generate_events(events_data, tracker: JobTracker, batch, sender, stop_event):
    for event in events_data["Events"]:
        if stop_event.is_set():
            return "stopped"

        if tracker.stop_requested():
            stop_event.set()
            return "stopped"

        if tracker.time_limit_reached():
            stop_event.set()
            return "time_limit"

        event["session_id"] = events_data["Session ID"]
        # event["user_id"] = events_data["User ID"]
        event["timestamp"] = time.time()

        try:
            await sender.send(event)
        except StreamError as err:
            # Stop every batch; the error is shown to the user in the UI
            tracker.fail(err)
            stop_event.set()
            return "failed"

        tracker.record_event(event, batch)

        random_time = random.randint(
            3, 5
        )  # Time gap of 3 to 5 seconds between each event
        await asyncio.sleep(random_time)


# Function to process a batch of session files
async def process_batch_of_files(s3_obj, file_keys, tracker: JobTracker, batch, sender, stop_event):
    for file_key in file_keys:
        if stop_event.is_set():
            return "failed" if tracker.failed else "stopped"

        try:
            data = s3_obj.get_data(bucket_name=BUCKET_NAME, key=file_key)
        except s3_obj.s3.exceptions.NoSuchKey:
            print(f"The specified file '{file_key}' does not exist in the bucket '{BUCKET_NAME}', skipping it.")
            continue

        # Convert JSON into dict to add event timestamp
        data_dict = json.loads(data)
        result = await generate_events(data_dict, tracker, batch, sender, stop_event)

        if result in ("stopped", "failed", "time_limit"):
            return result


async def data_generation(s3_obj, tracker: JobTracker, cloud_platform, cloud_parameters):
    stop_event = asyncio.Event()
    sender = None

    try:
        try:
            sender = create_sender(cloud_platform, cloud_parameters)
        except StreamError as err:
            tracker.fail(err)
            return {"message": "Data Generation failed!"}

        session_files_indexes = json.loads(
            s3_obj.get_data(bucket_name=BUCKET_NAME, key="session_files_indexes.json")
        )

        # Divide session file names round-robin into batches that stream concurrently
        session_keys = list(session_files_indexes.values())
        batches = [session_keys[i::NUM_BATCHES] for i in range(NUM_BATCHES)]

        # Process each batch using asyncio; all batches share the stream's single connection
        tasks = [
            asyncio.create_task(
                process_batch_of_files(s3_obj, batch_keys, tracker, batch_number, sender, stop_event)
            )
            for batch_number, batch_keys in enumerate(batches, start=1)
        ]

        result = await asyncio.gather(*tasks)
    except Exception as err:
        # Anything unexpected (S3, DynamoDB, bugs) must still end the job, or it stays in_progress forever
        tracker.fail(StreamError(
            f"Data generation stopped because of an error on our side. Please contact us with reference {tracker.job_id}.",
            SERVER,
            f"{type(err).__name__}: {err}"[:300],
        ))
        raise
    finally:
        if sender is not None:
            try:
                await sender.close()
            except Exception as err:
                print(f"Could not close the connection for {tracker.job_id}: {err}")

    if tracker.failed or "failed" in result:
        return {"message": "Data Generation failed!"}
    if "time_limit" in result:
        tracker.finish("stopped", stop_reason="time_limit")
        return {"message": "Data Generation stopped after reaching the time limit."}
    if "stopped" in result:
        tracker.finish("stopped", stop_reason="user")
        return {"message": "Work successfully stopped!"}

    # Once processing is completed, update the status to completed
    tracker.finish("completed")
    return {"message": "Data Generation completed!"}
