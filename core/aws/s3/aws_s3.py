import logging
from typing import List

import boto3

from core.aws.aws_config import AWSConfig

low_level_client = boto3.client("s3")
list_objects_paginator = low_level_client.get_paginator("list_objects_v2")


def upload_to_s3(
    s3_key: str, file_path: str, bucket: str = AWSConfig.S3_DEFAULT_BUCKET
) -> bool:
    try:
        low_level_client.upload_file(file_path, bucket, s3_key)
        return True

    except Exception as ex:
        logging.error(f"S3 Exception: {ex}")
        return False


def existing_on_s3(s3_key: str, bucket: str = AWSConfig.S3_DEFAULT_BUCKET) -> bool:
    try:
        resp = low_level_client.list_objects_v2(Bucket=bucket, Prefix=s3_key, MaxKeys=1)
        if resp and resp["KeyCount"] >= 1 and resp["Contents"][0]["Key"] == s3_key:
            return True
        return False

    except Exception as ex:
        logging.error(f"S3 Exception: {ex}")
        return False


def get_s3_keys_by_prefix(
    prefix: str, max_items: int = 200, bucket: str = AWSConfig.S3_DEFAULT_BUCKET
) -> (bool, List[str]):
    try:
        page_itr = list_objects_paginator.paginate(
            Bucket=bucket,
            Prefix=prefix,
            PaginationConfig={"MaxItems": max_items, "PageSize": 200},
        )
        return True, [
            content["Key"]
            for page in page_itr
            for content in page.get("Contents", [])
            if "Key" in content
        ]

    except Exception as ex:
        logging.error(f"S3 Exception: {ex}")
        return False, []


def download_file_from_s3(
    s3_key: str, downloaded_file_path: str, bucket: str = AWSConfig.S3_DEFAULT_BUCKET
) -> bool:
    try:
        low_level_client.download_file(bucket, s3_key, downloaded_file_path)
        return True

    except Exception as ex:
        logging.error(f"S3 Exception: {ex}")
        return False


def remove_from_s3_by_prefix(
    prefix: str, max_items: int = 200, bucket: str = AWSConfig.S3_DEFAULT_BUCKET
) -> bool:
    try:
        page_itr = list_objects_paginator.paginate(
            Bucket=bucket,
            Prefix=prefix,
            PaginationConfig={"MaxItems": max_items, "PageSize": 200},
        )
        removed_objects = [
            {"Key": content["Key"]}
            for page in page_itr
            for content in page.get("Contents", [])
            if "Key" in content
        ]
        if removed_objects:
            low_level_client.delete_objects(
                Bucket=bucket, Delete={"Objects": removed_objects}
            )
        return True

    except Exception as ex:
        logging.error(f"S3 Exception: {ex}")
        return False


def remove_from_s3(s3_key: str, bucket: str = AWSConfig.S3_DEFAULT_BUCKET) -> bool:
    try:
        low_level_client.delete_object(Bucket=bucket, Key=s3_key)
        return True

    except Exception as ex:
        logging.error(f"S3 Exception: {ex}")
        return False
