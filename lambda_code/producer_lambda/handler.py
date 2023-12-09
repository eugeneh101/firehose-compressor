import json
import os
import random
import string
import time
from datetime import datetime

import boto3


S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
S3_BUCKET_PREFIX_FOR_PRODUCER = os.environ["S3_BUCKET_PREFIX_FOR_PRODUCER"]

s3_resource = boto3.resource("s3")


def lambda_handler(event, context) -> None:
    for _ in range(random.choice(range(60))):
        now = datetime.utcnow().strftime("%Y-%m-%dT%H_%M_%S")
        json_file = s3_resource.Object(
            bucket_name=S3_BUCKET_NAME,
            key=f"{os.path.join(S3_BUCKET_PREFIX_FOR_PRODUCER, now)}.json",
        )
        payload = {
            "random_word": "".join(random.sample(string.ascii_lowercase, 5)),
            "random_float": random.random(),
        }
        json_file.put(Body=json.dumps(payload).encode("utf-8"))
        time.sleep(1)
