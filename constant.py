import os

# Once the request is sent, create a dynamodb object
if "AWS_REGION" not in os.environ:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv())

# Fetch the credentials from the environment variables
AWS_ACCESS_KEY_ID = os.environ.get("aws-access-key-id")
AWS_SECRET_ACCESS_KEY = os.environ.get("aws-secret-access-key")
AWS_REGION = os.environ.get("aws-region")
DYNAMODB_REGION = os.environ.get("dynamodb-region")

BUCKET_NAME = "enqurious-clickstream-data"
S3_FOLDER_PREFIX = ""
config_file_path = 'config.json'

# Live job progress (event feed, counters, errors, stop flag) - on-demand table, items expire via TTL
ACTIVITY_TABLE_NAME = "clickstream-job-activity"
ACTIVITY_TTL_SECONDS = 24 * 60 * 60
# An in_progress job with no event for this long is treated as dead (e.g. container restarted mid-job)
STALE_JOB_SECONDS = 180
# Streams stop automatically after this long, so forgotten streams don't run for weeks; users can start again
MAX_STREAM_SECONDS = 3 * 60 * 60