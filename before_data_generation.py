from datetime import datetime
from dynamod_db import DynamoDB
from fastapi import HTTPException
from constant import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    DYNAMODB_REGION,
    S3_FOLDER_PREFIX,
    BUCKET_NAME,
)
from s3 import S3
import asyncio
import threading
from data_generation import data_generation
from job_activity import JobActivity, JobTracker, make_job_id
from stream_errors import SERVER, StreamError, check_azure_connection, check_gcp_connection


def find_running_job(dynamodb, activity, email_id):
    """
    Returns the job_id of the user's running job, or None.

    A job still marked in_progress whose live activity has gone quiet (e.g. the container restarted
    mid-job) is marked "interrupted" so the user can start again.
    """
    try:
        in_progress = dynamodb.read_data_by_keys(email_id=email_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Please enter your email!")

    for item in in_progress:
        called_at = item["called_at"]["S"]
        job_id = make_job_id(email_id, called_at)
        summary = activity.get_summary(job_id)

        if summary is not None and not activity.is_stale(summary):
            return job_id

        # No live tracking means an older version started it, and that process no longer exists
        # (e.g. jobs left behind when the old EC2 server was removed), so it is safe to release
        if summary is not None:
            activity.update_summary(job_id, status="interrupted")
        dynamodb.update_data(email_id=email_id, called_at=called_at, status_value="interrupted")

    return None


async def before_data_generation(email_id: str, cloud_platform: str, **kwargs):
    if not email_id:
        raise HTTPException(status_code=400, detail="Please enter your email!")

    if cloud_platform == "Azure":
        cloud_parameters = {
            "hub_name": kwargs.get("hub_name"),
            "connection_string": kwargs.get("connection_string"),
        }
        target = cloud_parameters["hub_name"]
        if not cloud_parameters["connection_string"] or not cloud_parameters["hub_name"]:
            raise HTTPException(status_code=400, detail="Please enter the connection string and hub name.")
    elif cloud_platform == "GCP":
        cloud_parameters = {
            "credentials": kwargs.get("credentials"),
            "project_id": kwargs.get("project_id"),
            "topic_id": kwargs.get("topic_id"),
        }
        target = cloud_parameters["topic_id"]
        if not cloud_parameters["project_id"] or not cloud_parameters["topic_id"]:
            raise HTTPException(status_code=400, detail="Please enter the project ID and topic ID.")

    dynamodb = DynamoDB(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, DYNAMODB_REGION)
    activity = JobActivity(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, DYNAMODB_REGION)

    # Check whether the user already has a job running. If yes, point them to it instead of starting another
    running_job_id = find_running_job(dynamodb, activity, email_id)
    if running_job_id:
        print("work is in progress!")
        return {"message": "Work is in progress!", "job_id": running_job_id, "status": "in_progress"}

    # Verify the user's stream details before starting, so mistakes are reported immediately
    try:
        if cloud_platform == "Azure":
            await check_azure_connection(cloud_parameters["connection_string"], cloud_parameters["hub_name"])
        else:
            await check_gcp_connection(
                cloud_parameters["credentials"], cloud_parameters["project_id"], cloud_parameters["topic_id"]
            )
    except StreamError as err:
        raise HTTPException(status_code=400, detail=err.message)

    # User has sent the request, so let's add the user to the database
    # TODO: unaware about the broker_id - Thus by default broker is set to None, and it is not stored in database, once you aware about that, modify the function
    called_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dynamodb.write(email_id=email_id, called_at=called_at, status="in_progress")

    tracker = JobTracker(activity, dynamodb, email_id, called_at)
    activity.create_summary(tracker.job_id, email_id, cloud_platform, target)

    # Fetch data from s3
    s3_obj = S3(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    files = s3_obj.fetch_files(bucket_name=BUCKET_NAME, folder_prefix=S3_FOLDER_PREFIX)

    if not files:
        tracker.fail(StreamError(
            f"No source data is available right now. Please contact us with reference {tracker.job_id}.",
            SERVER,
            f"The s3 folder '{S3_FOLDER_PREFIX}' of Bucket '{BUCKET_NAME}' is empty",
        ))
        raise HTTPException(
            status_code=503,
            detail=f"The s3 folder '{S3_FOLDER_PREFIX}' of Bucket '{BUCKET_NAME}' is empty!!",
        )

    # Create a thread to run the loop in the background
    loop_thread = threading.Thread(
        target=asyncio.run,
        args=(data_generation(s3_obj, tracker, cloud_platform, cloud_parameters),),
        daemon=True,
    )
    loop_thread.start()

    return {"message": "Data Generation Started !!", "job_id": tracker.job_id, "status": "in_progress"}
