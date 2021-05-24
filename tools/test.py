import logging
logging.basicConfig(level=logging.DEBUG)
import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

slack_token = "xoxb-1547184268790-2078677029527-wppHc8QcA3zR5qmeJH2bjqHf"
client = WebClient(token=slack_token)


def post_message(chanel: str, text: str):
    try:
        response = client.chat_postMessage(
            channel=chanel,
            text=text,
        )
    except Exception as e:
        print(e)


if __name__ == "__main__":
    post_message(chanel="test", text="hello world 4")

# url = "https://docs.google.com/spreadsheets/d/1FNfYZjn9LNeCUus4JbLdAJ5qSrmFJGYVVhsbjaPAD4g/edit#gid=0"








