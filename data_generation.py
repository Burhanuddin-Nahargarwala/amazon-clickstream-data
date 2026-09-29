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


async def azure_data_generation(hub_name, connection_string, data_dict):
    """
    Sends one event to Azure Event Hubs.

    Parameters:
    - `hub_name` (str): The name of the Azure Event Hub.
    - `connection_string` (str): The connection string for the Azure Event Hubs namespace.
    - `data_dict` (dict): The event to be sent, represented as a dictionary.

    Raises:
    - StreamError: A user-readable description of why the event could not be sent.
    """
    try:
        producer = EventHubProducerClient.from_connection_string(
            connection_string, eventhub_name=hub_name
        )

        async with producer:
            event_data_batch = await producer.create_batch()

            event_data_batch.add(EventData(json.dumps(data_dict)))

            await producer.send_batch(event_data_batch)
            print(f"Event sent to Azure Event Hub: {data_dict}")

    except Exception as err:
        raise describe_azure_error(err, hub_name) from err


async def gcp_data_generation(credentials, project_id, topic_id, data_dict):
    """
    Publishes one event to a Google Pub/Sub topic.

    Parameters:
    - `credentials` (dict): The GCP service account key as a dictionary.
    - `project_id` (str): The GCP project ID.
    - `topic_id` (str): The GCP topic ID to which the message will be published.
    - `data_dict` (dict): The event to be published, represented as a dictionary.

    Raises:
    - StreamError: A user-readable description of why the event could not be published.
    """
    try:
        publisher = pubsub_v1.PublisherClient.from_service_account_info(credentials)
        topic_path = publisher.topic_path(project_id, topic_id)

        data_str = json.dumps(data_dict)
        data_encoded_str = data_str.encode("utf-8")
        future = publisher.publish(topic_path, data_encoded_str)
        message_id = future.result()  # Wait for the publish operation to complete

        print(f"Published {data_encoded_str} to {topic_path}: {message_id}")
    except Exception as err:
        raise describe_gcp_error(err, project_id, topic_id) from err


async def generate_events(events_data, tracker: JobTracker, batch, cloud_platform, cloud_parameters, stop_event):
    for event in events_data["Events"]:
        if stop_event.is_set():
            return "stopped"

        if tracker.stop_requested():
            stop_event.set()
            return "stopped"

        event["session_id"] = events_data["Session ID"]
        # event["user_id"] = events_data["User ID"]
        event["timestamp"] = time.time()

        try:
            if cloud_platform == "Azure":
                await azure_data_generation(
                    cloud_parameters.get("hub_name"),
                    cloud_parameters.get("connection_string"),
                    event,
                )
            elif cloud_platform == "GCP":
                await gcp_data_generation(
                    cloud_parameters.get("credentials"),
                    cloud_parameters.get("project_id"),
                    cloud_parameters.get("topic_id"),
                    event,
                )
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
async def process_batch_of_files(s3_obj, file_keys, tracker: JobTracker, batch, cloud_platform, cloud_parameters, stop_event):
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
        result = await generate_events(
            data_dict, tracker, batch, cloud_platform, cloud_parameters, stop_event
        )  # Run event generation asynchronously

        if result in ("stopped", "failed"):
            return result


async def data_generation(s3_obj, tracker: JobTracker, cloud_platform, cloud_parameters):
    stop_event = asyncio.Event()

    try:
        session_files_indexes = json.loads(
            s3_obj.get_data(bucket_name=BUCKET_NAME, key="session_files_indexes.json")
        )

        # Divide session file names round-robin into batches that stream concurrently
        session_keys = list(session_files_indexes.values())
        batches = [session_keys[i::NUM_BATCHES] for i in range(NUM_BATCHES)]

        # Process each batch using asyncio
        tasks = [
            asyncio.create_task(
                process_batch_of_files(
                    s3_obj, batch_keys, tracker, batch_number, cloud_platform, cloud_parameters, stop_event
                )
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

    if tracker.failed or "failed" in result:
        return {"message": "Data Generation failed!"}
    if "stopped" in result:
        tracker.finish("stopped")
        return {"message": "Work successfully stopped!"}

    # Once processing is completed, update the status to completed
    tracker.finish("completed")
    return {"message": "Data Generation completed!"}
