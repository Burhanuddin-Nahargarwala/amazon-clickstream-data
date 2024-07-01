from datetime import datetime
from api_config import read_config, update_config
from dynamod_db import DynamoDB
from fastapi import HTTPException
from constant import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    DYNAMODB_REGION,
    S3_FOLDER_PREFIX,
    BUCKET_NAME,
    config_file_path,
)
import threading
from s3 import S3
import asyncio
import threading
from data_generation import data_generation


def before_data_generation(email_id: str, cloud_platform: str, **kwargs):
    config = read_config(config_file_path)
    config["stop_flag"] = False
    config["stop_email_id"] = ""

    # Save the updated configuration
    update_config(config_file_path, config)

    # Once the request is sent, create a dynamodb object
    # if code is running on AWS EC2 instance, no need to pass endpoint_url ("AWS_REGION" in os.environ)
    # else pass endpoint_url
    dynamodb = DynamoDB(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, DYNAMODB_REGION)

    # Check whether the user had already sent the request and his work is in progress. If yes, then skip
    # the request of that user
    try:
        user_in_progress = dynamodb.read_data_by_keys(email_id=email_id)
    except Exception as err:
        # print(err)
        raise HTTPException(status_code=400, detail=f"Please enter your email!")

    if user_in_progress:
        print("work is in progress!")
        return {"message": "Work is in progress!"}

    # User has sent the request, so let's add the user to the database
    # TODO: unaware about the broker_id - Thus by default broker is set to None, and it is not stored in database, once you aware about that, modify the function
    called_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dynamodb.write(email_id=email_id, called_at=called_at, status="in_progress")

    # Fetch data from s3
    s3_obj = S3(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    # Example usage
    files = s3_obj.fetch_files(bucket_name=BUCKET_NAME, folder_prefix=S3_FOLDER_PREFIX)

    if files is None:
        dynamodb.update_data(
            email_id=email_id, called_at=called_at, status_value="failed"
        )
        raise HTTPException(
            status_code=503,
            detail=f"The s3 folder '{S3_FOLDER_PREFIX}' of Bucket '{BUCKET_NAME}' is empty!!",
        )

    # Create a thread to run the loop in the background
    if cloud_platform == "Azure":
        cloud_parameters = {
            "hub_name": kwargs.get("hub_name"),
            "connection_string": kwargs.get("connection_string")
        }
    elif cloud_platform == "GCP":
        cloud_parameters = {
            "credentials": kwargs.get("credentials"),
            "project_id": kwargs.get("project_id"),
            "topic_id": kwargs.get("topic_id")
        }

    loop_thread = threading.Thread(
        target=asyncio.run,
        args=(
            data_generation(
                s3_obj, dynamodb, email_id, called_at, cloud_platform, cloud_parameters=cloud_parameters
            ),
        ),
    )
    loop_thread.start()

    return {"message": "Data Generation Successfully!"}
