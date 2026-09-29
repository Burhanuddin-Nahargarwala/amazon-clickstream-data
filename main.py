from fastapi import FastAPI, Request, HTTPException, Depends, Body, Query
from fastapi.templating import Jinja2Templates
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from s3 import S3
import urllib.parse
import random
import time
import json
import os
import mimetypes

# Python 3.11 doesn't know .webp, so the logo would be served as text/plain
mimetypes.add_type("image/webp", ".webp")

# from mangum import Mangum
import asyncio
from pathlib import Path

# import eventhub_upload
from before_data_generation import before_data_generation
from dynamod_db import DynamoDB
from job_activity import JobActivity, make_job_id
import auth
from constant import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    DYNAMODB_REGION,
    BUCKET_NAME,
)

# create the FastAPI object
app = FastAPI()

app.mount(
    "/static",
    StaticFiles(directory=Path(__file__).parent.absolute() / "static"),
    name="static",
)
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# Browsers request /favicon.ico on their own, regardless of the <link> tags
@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse(Path(__file__).parent.absolute() / "static" / "favicon.ico")


@app.post("/stop_processing", dependencies=[Depends(auth.get_api_key)])
async def stop_processing(details: dict = Body(...)):
    # fetch email_id
    email_id = details.get("email_id")

    # Create dynamodb instances
    dynamodb = DynamoDB(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, DYNAMODB_REGION)
    activity = JobActivity(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, DYNAMODB_REGION)

    # Find the user's running work, if any
    try:
        user_in_progress = dynamodb.read_data_by_keys(email_id=email_id)
    except Exception as err:
        raise HTTPException(status_code=400, detail=f"Please enter your email!")

    if not user_in_progress:
        return {"message": "No data generation is running for this email."}

    for item in user_in_progress:
        called_at = item["called_at"]["S"]
        job_id = make_job_id(email_id, called_at)
        summary = activity.get_summary(job_id)

        if summary is None or activity.is_stale(summary):
            # Nothing is actually running this job any more (older version, or the container restarted),
            # so close it directly instead of waiting for a worker to notice the stop flag
            if summary is not None:
                activity.update_summary(job_id, status="stopped", stop_requested=True)
            dynamodb.update_data(email_id=email_id, called_at=called_at, status_value="stopped")
        else:
            # The running job checks this flag every few seconds and stops itself
            activity.update_summary(job_id, stop_requested=True)

    return {"message": "Data Generation Stopped!"}


@app.get("/jobs/latest", dependencies=[Depends(auth.get_api_key)])
async def latest_job(email_id: str = Query(...)):
    """Returns the user's most recent job, so the UI can reconnect to it after a page reload."""
    dynamodb = DynamoDB(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, DYNAMODB_REGION)
    item = dynamodb.read_latest_by_email(email_id)
    if item is None:
        raise HTTPException(status_code=404, detail="No data generation found for this email.")

    called_at = item["called_at"]["S"]
    return {
        "job_id": make_job_id(email_id, called_at),
        "called_at": called_at,
        "status": item.get("status", {}).get("S"),
    }


@app.get("/jobs/activity", dependencies=[Depends(auth.get_api_key)])
async def job_activity(job_id: str = Query(...), after: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=200)):
    """
    Returns the job's live status plus the events sent after sequence number `after`.
    The UI polls this, passing the last `seq` it has already shown.
    """
    activity = JobActivity(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, DYNAMODB_REGION)
    summary = activity.get_summary(job_id)
    if summary is None:
        raise HTTPException(status_code=404, detail="Live activity is not available for this job.")

    events = activity.get_events(job_id, after=after, limit=limit)
    for event in events:
        event["payload"] = json.loads(event["payload"])
        event.pop("job_id", None)
        event.pop("expires_at", None)

    for key in ("seq", "expires_at", "email_id"):
        summary.pop(key, None)

    return {
        "job": summary,
        "events": events,
        "last_seq": events[-1]["seq"] if events else after,
    }


@app.post("/data_generation", dependencies=[Depends(auth.get_api_key)])
async def start_data_generation(details: dict = Body(...)):
    # Extract details
    cloud_provider = details.get("cloud_provider")

    if cloud_provider == "Azure":
        connection_string = details.get("connection_string")
        hub_name = details.get("hub_name")
        email_id = details.get("email_id")

        message = await before_data_generation(
            email_id,
            cloud_platform="Azure",
            connection_string=connection_string,
            hub_name=hub_name,
        )
    elif cloud_provider == "GCP":
        credentials_str = details.get("credentials")
        project_id = details.get("project_id")
        topic_id = details.get("topic_id")
        email_id = details.get("email_id")

        try:
            credentials_dict = json.loads(credentials_str)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="Please upload a valid service account key (JSON file).")

        message = await before_data_generation(
            email_id,
            cloud_platform="GCP",
            credentials=credentials_dict,
            project_id=project_id,
            topic_id=topic_id,
        )
    else:
        raise HTTPException(status_code=400, detail="Invalid cloud provider specified!")

    # Now check whether endpoint contains the text 'Endpoint=' or not, if not then add that
    # connection_string = urllib.parse.unquote(connection_string)
    # if not connection_string.startswith("Endpoint="):
    #     connection_string = f"Endpoint={connection_string}"

    return message


# if __name__ == "__main__":
#     import uvicorn
#     from dotenv import load_dotenv, find_dotenv

#     # Once the request is sent, create a dynamodb object
#     if "AWS_REGION" not in os.environ:
#         load_dotenv(find_dotenv())

#     uvicorn.run(app, host="127.0.0.1", port=8001, reload=True)
