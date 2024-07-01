import os

# Fetch the credentials from the environment variables
AWS_ACCESS_KEY_ID = os.environ.get("aws-access-key-id")
AWS_SECRET_ACCESS_KEY = os.environ.get("aws-secret-access-key")
AWS_REGION = os.environ.get("aws-region")
DYNAMODB_REGION = os.environ.get("dynamodb-region")

BUCKET_NAME = "enqurious-clickstream-data"
S3_FOLDER_PREFIX = ""
config_file_path = 'config.json'