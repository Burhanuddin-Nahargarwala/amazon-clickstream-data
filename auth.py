from fastapi import Security, HTTPException
from fastapi.security.api_key import APIKeyHeader
import os
from dotenv import load_dotenv, find_dotenv


if 'AWS_REGION' not in os.environ:
        load_dotenv(find_dotenv())


ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")

## Now fetch the api_key from the header
api_key_header = APIKeyHeader(name="access_token", auto_error=False)

async def get_api_key(api_key_header: str = Security(api_key_header)):
    if api_key_header == ACCESS_TOKEN:
        return api_key_header   
    else:
        raise HTTPException(
            status_code=401, detail="Could not validate API KEY"
        )