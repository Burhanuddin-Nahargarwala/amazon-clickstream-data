from fastapi import FastAPI, Request, HTTPException, Depends, Body
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from s3 import S3
import urllib.parse
import random
import time
import json
import os

# from mangum import Mangum
import asyncio
from pathlib import Path

# import eventhub_upload
from api_config import read_config, update_config
from before_data_generation import before_data_generation
from dynamod_db import DynamoDB
import auth
from constant import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    DYNAMODB_REGION,
    config_file_path,
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


@app.post("/stop_processing", dependencies=[Depends(auth.get_api_key)])
async def stop_processing(details: dict = Body(...)):
    # fetch email_id
    email_id = details.get("email_id")

    # Create a dynamodb instance
    dynamodb = DynamoDB(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, DYNAMODB_REGION)

    # Check whether the user had already sent the request and his work is in progress. If yes, then skip
    # the request of that user
    try:
        user_in_progress = dynamodb.read_data_by_keys(email_id=email_id)
    except Exception as err:
        raise HTTPException(status_code=400, detail=f"Please enter your email!")

    if user_in_progress:
        # Modify the configuration
        config = read_config(config_file_path)
        config["stop_flag"] = True
        config["stop_email_id"] = email_id

        # Save the updated configuration
        update_config(config_file_path, config)

    return {"message": "Data Generation Stopped!"}


@app.post("/data_generation", dependencies=[Depends(auth.get_api_key)])
async def start_data_generation(details: dict = Body(...)):
    # Extract details
    cloud_provider = details.get("cloud_provider")

    if cloud_provider == "Azure":
        connection_string = details.get("connection_string")
        hub_name = details.get("hub_name")
        email_id = details.get("email_id")

        message = before_data_generation(
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

        credentials_dict = json.loads(credentials_str)
        message = before_data_generation(
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


if __name__ == "__main__":
    import uvicorn
    from dotenv import load_dotenv, find_dotenv

    # Once the request is sent, create a dynamodb object
    if "AWS_REGION" not in os.environ:
        load_dotenv(find_dotenv())

    uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)
