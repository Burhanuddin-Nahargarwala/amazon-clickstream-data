from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from s3 import S3
import urllib.parse
import random
import time
from datetime import datetime, timedelta
import json
import os

# from mangum import Mangum
import asyncio
import threading
from pathlib import Path

from constant import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    DYNAMODB_REGION,
    config_file_path,
    BUCKET_NAME
)

# create the FastAPI object
app = FastAPI()

async def generate_events(events_data):
    for event in events_data["Events"]:
        event["session_id"] = events_data["Session ID"]
        # event["user_id"] = events_data["User ID"]
        event["timestamp"] = time.time()
        print(event)

        random_time = random.randint(
            3, 5
        )  # Time gap of 3 to 5 seconds between each event
        await asyncio.sleep(random_time)


# Function to process a batch of session files
async def process_batch_of_files(s3_obj, file_keys):
    for file_key in file_keys:
        try:
            data = s3_obj.get_data(bucket_name=BUCKET_NAME, key=file_key)
        except s3_obj.s3.exceptions.NoSuchKey as e:
            raise HTTPException(
                status_code=503,
                detail=f"The specified file '{file_key}' does not exist in the bucket '{BUCKET_NAME}'.",
            )

        # Convert JSON into dict to add event timestamp
        data_dict = json.loads(data)
        await generate_events(data_dict)  # Run event generation asynchronously


async def data_generation():
    # Fetch data from s3
    s3_obj = S3(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

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
        tasks.append(asyncio.create_task(process_batch_of_files(s3_obj, batch)))

    await asyncio.gather(*tasks)


@app.get(
    "/data_generation"
)  # Correct method should be POST. @app.post(), Change it later
async def start_data_generation():
    # Create a thread to run the background task
    loop_thread = threading.Thread(target=asyncio.run, args=(data_generation(),))
    loop_thread.start()
    return {"message": "Data Generation Successfully!"}


if __name__ == "__main__":
    import uvicorn
    from dotenv import load_dotenv, find_dotenv

    # Once the request is sent, create a dynamodb object
    if "AWS_REGION" not in os.environ:
        load_dotenv(find_dotenv())

    uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)
