import json
import logging
import traceback
from typing import List

import boto3

from app.biz.aws.aws_config import AWSConfig

aws_session = boto3.Session(region_name=AWSConfig.SNS_DEFAULT_REGION)


def push_notification_to_sns_endpoints(sns_endpoints: List[str], message):
    sns_client = aws_session.client("sns")

    for sns_endpoint in sns_endpoints:
        # noinspection PyBroadException
        try:
            sns_client.publish(TargetArn=sns_endpoint, MessageStructure="string", Message=json.dumps(message))

        except Exception:
            logging.debug("Failed to send push notification via SNS")
            logging.debug(traceback.format_exc())
