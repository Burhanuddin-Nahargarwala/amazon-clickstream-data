from datetime import datetime
from modify_config_file import read_config, update_config
from constant import (
    S3_FOLDER_PREFIX,
    BUCKET_NAME,
    config_file_path,
)
from fastapi import HTTPException
import json
import random
import time
from google.cloud import pubsub_v1

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
import asyncio


STOP_EVENT = False


async def azure_data_generation(
    dynamodb, email_id, called_at, hub_name, connection_string, data_dict
):
    """
    Generates data for Azure Event Hubs by creating an event producer and sending data to the specified
    event hub using the provided connection string.

    Parameters:
    - `dynamodb`: The DynamoDB object.
    - `email_id` (str): The email ID associated with the data generation request.
    - `called_at` (str): The timestamp when the data generation process was initiated.
    - `hub_name` (str): The name of the Azure Event Hub.
    - `connection_string` (str): The connection string for the Azure Event Hubs.
    - `data_dict` (dict): The data to be sent, represented as a dictionary.

    Raises:
    - HTTPException (status_code=403): If an exception occurs during the creation of the event producer,
      the function updates the DynamoDB record with a 'failed' status and raises an HTTPException.

    Note:
    The function attempts to create an event producer using the provided connection string and event hub name.
    If successful, it sends the serialized JSON data to the Azure Event Hub using the created event producer.
    If an exception occurs during the process, the DynamoDB record is updated with a 'failed' status, and an
    HTTPException with a status code of 403 is raised, providing details about the error.
    """
    # create event producer
    try:
        # producer = eventhub_upload.create_event_producer(
        #     connection_string=urllib.parse.unquote(connection_string),
        #     event_hub_name=hub_name,
        # )

        producer = EventHubProducerClient.from_connection_string(
            connection_string, eventhub_name=hub_name
        )

        async with producer:
            event_data_batch = await producer.create_batch()

            event_data_batch.add(EventData(json.dumps(data_dict)))

            await producer.send_batch(event_data_batch)
            print(f"Event sent to Azure Event Hub: {data_dict}")

    except Exception:
        dynamodb.update_data(
            email_id=email_id, called_at=called_at, status_value="failed"
        )
        raise HTTPException(
            status_code=403,
            detail="Check whether the connection string or event hub name is correct or not",
        )

    # now let's upload the data to connection string
    # await eventhub_upload.send_to_eventhub(event=json.dumps(data_dict), producer=producer)


async def gcp_data_generation(
    dynamodb, email_id, called_at, credentials, project_id, topic_id, data_dict
):
    """
    Generates data for GCP (Google Cloud Platform) by publishing a message to a specified topic. 

    Parameters:
    - `dynamodb`: The DynamoDB object.
    - `email_id` (str): The email ID associated with the data generation request.
    - `called_at` (str): The timestamp when the data generation process was initiated.
    - `credentials` (dict): The GCP credentials as a dictionary.
    - `project_id` (str): The GCP project ID.
    - `topic_id` (str): The GCP topic ID to which the message will be published.
    - `data_dict` (dict): The data to be published, represented as a dictionary.
    
    Note: The function creates a publisher using the provided GCP credentials and publishes the serialized 
    JSON data to the specified GCP topic.
    If an exception occurs during the process, the DynamoDB record is updated with a 'failed' status, and an
    HTTPException with a status code of 403 is raised, providing details about the error.
    """

    # Create a publisher from the credentials
    # create event producer
    try:
        publisher = pubsub_v1.PublisherClient.from_service_account_info(credentials)
        topic_path = publisher.topic_path(project_id, topic_id)

        data_str = json.dumps(data_dict)
        data_encoded_str = data_str.encode("utf-8")
        future = publisher.publish(topic_path, data_encoded_str)
        future.result()  # Wait for the publish operation to complete asynchronously

        print(f"Published {data_encoded_str} to {topic_path}: {future.result()}")
    except Exception:
        dynamodb.update_data(
            email_id=email_id, called_at=called_at, status_value="failed"
        )
        raise HTTPException(
            status_code=403,
            detail="Check whether the connection string or event hub name is correct or not",
        )


async def generate_events(
    events_data, email_id, dynamodb, called_at, cloud_platform, cloud_parameters, stop_event
):
    # global STOP_EVENT

    for event in events_data["Events"]:
        if stop_event.is_set():
            return "stopped"
        
        config = read_config(config_file_path)
        stop_flag = config["stop_flag"]
        stop_email_id = config["stop_email_id"]

        if stop_flag and stop_email_id == email_id:
            stop_event.set()
            return "stopped"

        event["session_id"] = events_data["Session ID"]
        # event["user_id"] = events_data["User ID"]
        event["timestamp"] = time.time()
        # print(event)

        if cloud_platform == "Azure":
            hub_name = cloud_parameters.get("hub_name")
            connection_string = cloud_parameters.get("connection_string")

            # create event producer
            await azure_data_generation(
                dynamodb, email_id, called_at, hub_name, connection_string, event
            )
        elif cloud_platform == "GCP":
            credentials = cloud_parameters.get("credentials")
            project_id = cloud_parameters.get("project_id")
            topic_id = cloud_parameters.get("topic_id")

            # create event producer
            await gcp_data_generation(
                dynamodb,
                email_id,
                called_at,
                credentials,
                project_id,
                topic_id,
                event,
            )

        random_time = random.randint(
            3, 5
        )  # Time gap of 3 to 5 seconds between each event
        await asyncio.sleep(random_time)


# Function to process a batch of session files
async def process_batch_of_files(
    s3_obj, file_keys, email_id, dynamodb, called_at, cloud_platform, cloud_parameters, stop_event
):
    # global STOP_EVENT

    for file_key in file_keys:

        # if STOP_EVENT:
        #     return "stopped"

        if stop_event.is_set():
            return "stopped"

        try:
            data = s3_obj.get_data(bucket_name=BUCKET_NAME, key=file_key)
        except s3_obj.s3.exceptions.NoSuchKey as e:
            raise HTTPException(
                status_code=503,
                detail=f"The specified file '{file_key}' does not exist in the bucket '{BUCKET_NAME}'.",
            )

        # Convert JSON into dict to add event timestamp
        data_dict = json.loads(data)
        result = await generate_events(
            data_dict, email_id, dynamodb, called_at, cloud_platform, cloud_parameters, stop_event
        )  # Run event generation asynchronously

        if result == "stopped":
            return "stopped"


async def data_generation(
    s3_obj, dynamodb, email_id, called_at, cloud_platform, cloud_parameters
):
    stop_event = asyncio.Event()

    # Example usage
    session_files_indexes = json.loads(
        s3_obj.get_data(bucket_name=BUCKET_NAME, key="session_files_indexes.json")
    )

    # Divide session file names into batches
    num_batches = 4  # Example: Split into 4 batches
    batch_size = len(session_files_indexes) // num_batches
    # batches = [list(session_files_indexes.values())[i:i+batch_size] for i in range(0, len(session_files_indexes), batch_size)]

    batch_files_mapping = {}
    i = 0
    for _ in range(0, len(session_files_indexes), batch_size):
        i += 1
        batch_files_mapping[f"batch_{i}"] = []

    session_keys = list(session_files_indexes.values())
    batch_index = 0
    for key in session_keys:
        batch_index += 1
        batch_files_mapping[f"batch_{batch_index}"].append(key)

        if batch_index == 5:
            batch_index = 0

    batches = list(
        batch_files_mapping.values()
    )  # batches are divided like this 10877 + 10877 + 10877 + 10876 + 10876

    # Process each batch using asyncio
    tasks = []
    for batch in batches:
        tasks.append(
            asyncio.create_task(
                process_batch_of_files(
                    s3_obj,
                    batch,
                    email_id,
                    dynamodb,
                    called_at,
                    cloud_platform,
                    cloud_parameters,
                    stop_event
                )
            )
        )

    result = await asyncio.gather(*tasks)

    if "stopped" in result:
        dynamodb.update_data(
            email_id=email_id, called_at=called_at, status_value="stopped"
        )
        # read the config files, and set stop_flag and stop_email_id to null
        config = read_config(config_file_path)
        config["stop_flag"] = False
        config["stop_email_id"] = ""

        # Save the updated configuration
        update_config(config_file_path, config)

        # print("stop_flag:", stop_flag)
        # print("stop_email_id:", stop_email_id)
        return {"message": "Work successfully stopped!"}
    else:
        # Once processing is completed, update the status to completed
        dynamodb.update_data(
            email_id=email_id, called_at=called_at, status_value="completed"
        )

        # read the config files, and set stop_flag and stop_email_id to null
        config = read_config(config_file_path)
        config["stop_flag"] = False
        config["stop_email_id"] = ""

        # Save the updated configuration
        update_config(config_file_path, config)

        return {"message": "Data Generation completed!"}
