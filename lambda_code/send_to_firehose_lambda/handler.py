import json
import os
import random
import string
import time
from datetime import datetime

import boto3

S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
S3_BUCKET_PREFIX_FOR_PRODUCER = os.environ["S3_BUCKET_PREFIX_FOR_PRODUCER"]
FIREHOSE_NAME = os.environ["FIREHOSE_NAME"]

s3_resource = boto3.resource("s3")
firehose_client = boto3.client("firehose")


def lambda_handler(event, context) -> None:
    assert event["source"] == "aws.s3"
    assert event["detail-type"] == "Object Created"
    assert event["detail"]["bucket"]["name"] == S3_BUCKET_NAME
    assert event["detail"]["object"]["key"].startswith(S3_BUCKET_PREFIX_FOR_PRODUCER)
    key = event["detail"]["object"]["key"]
    json_file = s3_resource.Object(bucket_name=S3_BUCKET_NAME, key=key)
    file_content_bytes = json_file.get()["Body"].read()
    response = firehose_client.put_record(
        DeliveryStreamName=FIREHOSE_NAME,
        Record={"Data": file_content_bytes},
    )
    print(f'Wrote the following into "{FIREHOSE_NAME}": ', file_content_bytes)
