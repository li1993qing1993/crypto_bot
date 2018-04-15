import boto3
import botocore

BUCKET_NAME = '' # replace with your bucket name
KEY = '' # replace with your object key


def download_secret(file_path):
    s3 = boto3.resource('s3')

    try:
        s3.Bucket(BUCKET_NAME).download_file(KEY, file_path)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise
