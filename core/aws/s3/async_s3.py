from typing import List
import asyncio
import logging

from app.biz.aws.aws_config import AWSConfig
from app.biz.aws.auto_async_client import auto_s3_async_client
from app.biz.aws.s3.aws_s3 import existing_on_s3

# https://aiobotocore.readthedocs.io


@auto_s3_async_client
async def upload_to_s3_async(client, s3_key: str, file_path: str, bucket: str = AWSConfig.S3_DEFAULT_BUCKET) -> bool:
    try:
        with open(file_path, "rb") as f:
            await client.put_object(Bucket=bucket, Key=s3_key, Body=f)
        return True

    except Exception as ex:
        logging.error(f"S3 Exception: {ex}")
        return False


@auto_s3_async_client
async def download_from_s3_async(
    client, s3_key: str, downloaded_file_path: str, bucket: str = AWSConfig.S3_DEFAULT_BUCKET
) -> bool:
    try:
        response = await client.get_object(Bucket=bucket, Key=s3_key)
        async with response["Body"] as stream:
            with open(downloaded_file_path, "wb") as f:
                f.write(await stream.read())
        return True

    except Exception as ex:
        logging.error(f"S3 Exception: {ex}")
        return False


@auto_s3_async_client
async def remove_from_s3_async(client, s3_key: str, bucket: str = AWSConfig.S3_DEFAULT_BUCKET) -> bool:
    try:
        await client.delete_object(Bucket=bucket, Key=s3_key)
        return True
    except Exception as ex:
        logging.error(f"S3 Exception: {ex}")
        return False


@auto_s3_async_client
async def remove_by_keys_async(client, keys: List[str], bucket: str = AWSConfig.S3_DEFAULT_BUCKET):
    tasks = []
    for key in keys:
        if existing_on_s3(key):
            tasks.append(asyncio.create_task(remove_from_s3_async(key)))
    if tasks:
        await asyncio.gather(*tasks)
