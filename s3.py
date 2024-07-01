import boto3
from io import StringIO
import os

class S3:
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name):
        self.s3 = boto3.client(
            service_name="s3",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )


    def fetch_bucket(self):
        # Fetch the buckets list
        response = self.s3.list_buckets()

        # Get a list of all the bucket names from the response
        response_buckets = response["Buckets"]
        if response_buckets:
            buckets = [bucket['Name'] for bucket in response_buckets]
        else:
            buckets = None

        ## return the bucket list
        return buckets


    def fetch_objects(self, bucket_name):
        ## Fetch the objects list
        response = self.s3.list_objects(Bucket=bucket_name)

        ## Get list of all the objects
        response_objects = response["Contents"]

        ## Get a list of all the object keys from the response
        if response_objects:
            objects = [single_object["Key"] for single_object in response_objects]
        else:
            objects = None

        return objects

    def get_data(self, bucket_name, key):
        ## Now read the file using get_object
        file = self.s3.get_object(Bucket=bucket_name, Key=key)
        
        ## Now fetch the content from the body
        data = file['Body'].read().decode('utf-8')

        return data

        # final_data = StringIO(data.decode('utf-8'))

        # return final_data

    def upload_data(self, bucket_name, json_data, key: str):
        # Upload the JSON data to S3
        self.s3.put_object(Bucket=bucket_name, Key=key, Body=json_data)

    def fetch_files(self, bucket_name, folder_prefix):
        # List objects in the specified folder
        try:
            response = self.s3.list_objects_v2(
                Bucket=bucket_name, 
                Prefix=folder_prefix)

            # Extract the object keys (file names)
            object_keys = [obj['Key'] for obj in response.get('Contents', [])]

        except Exception as e:
            print(f"Error: {str(e)}")
            object_keys = None
        
        return object_keys