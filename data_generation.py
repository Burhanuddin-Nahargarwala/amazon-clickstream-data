from datetime import datetime
from api_config import read_config, update_config
from constant import (
    S3_FOLDER_PREFIX,
    BUCKET_NAME,
    config_file_path,
)
from fastapi import HTTPException
import json
import random
import time
from datetime import datetime, timedelta
import urllib
from google.cloud import pubsub_v1

import eventhub_upload
from dynamod_db import DynamoDB
import asyncio


STOP_EVENT = False


async def generate_events(events_data, email_id):
    global STOP_EVENT

    for event in events_data["Events"]:
        config = read_config(config_file_path)
        stop_flag = config["stop_flag"]
        stop_email_id = config["stop_email_id"]

        if stop_flag and stop_email_id == email_id:
            STOP_EVENT=True
            return
        
        event["session_id"] = events_data["Session ID"]
        # event["user_id"] = events_data["User ID"]
        event["timestamp"] = time.time()
        print(event)

        random_time = random.randint(
            3, 5
        )  # Time gap of 3 to 5 seconds between each event
        await asyncio.sleep(random_time)


# Function to process a batch of session files
async def process_batch_of_files(s3_obj, file_keys, email_id):
    global STOP_EVENT

    for file_key in file_keys:
        # config = read_config(config_file_path)
        # stop_flag = config["stop_flag"]
        # stop_email_id = config["stop_email_id"]

        if STOP_EVENT:
            return "stopped"

        # if stop_flag and stop_email_id == email_id:
        #     STOP_EVENT=True
        #     return "stopped"
        #         # IS_STOP = True
        #         # return stop_data_generation(
        #         #     dynamodb, config, stop_flag, stop_email_id, called_at
        #         # )

        try:
            data = s3_obj.get_data(bucket_name=BUCKET_NAME, key=file_key)
        except s3_obj.s3.exceptions.NoSuchKey as e:
            raise HTTPException(
                status_code=503,
                detail=f"The specified file '{file_key}' does not exist in the bucket '{BUCKET_NAME}'.",
            )

        # Convert JSON into dict to add event timestamp
        data_dict = json.loads(data)
        await generate_events(data_dict, email_id)  # Run event generation asynchronously


async def data_generation(
    s3_obj, dynamodb, email_id, called_at, cloud_platform, cloud_parameters
):
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
                process_batch_of_files(s3_obj, batch, email_id)
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
