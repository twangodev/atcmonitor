import io
import tarfile

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from tqdm import tqdm

from cache import read_cache, write_cache

def get_tar_references(
        bucket="data-samples",
        prefix="states/",
        endpoint="https://s3.opensky-network.org",
):
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        config=Config(signature_version=UNSIGNED),
    )

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    csv_tars = [
        obj["Key"]
        for page in tqdm(pages, desc="Listing S3 Pages", unit="file")
        for obj in page.get("Contents", [])
        if (
            obj["Key"].lower().endswith(".tar")
            and obj["Size"] > 1024 ** 2  # at least 1 MB
            and "csv" in obj["Key"].lower()
        )
    ]

    handlers = []
    for key in csv_tars:
        def make_handler(k):
            def fetch_and_extract():
                data = read_cache(k)
                if data is None:
                    resp = s3.get_object(Bucket=bucket, Key=k)
                    data = resp["Body"].read()
                    write_cache(k, data)

                buf = io.BytesIO(data)
                result = {}
                with tarfile.open(fileobj=buf, mode="r:*") as tar:
                    for member in tar.getmembers():
                        if member.isreg():
                            f = tar.extractfile(member)
                            result[member.name] = f.read()
                return result

            return fetch_and_extract

        handlers.append((key, make_handler(key)))

    return handlers
