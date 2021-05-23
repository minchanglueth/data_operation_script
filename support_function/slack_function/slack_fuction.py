import logging
logging.basicConfig(level=logging.DEBUG)
from slack_sdk import WebClient

# Get application first: https://api.slack.com/apps/A02341UQV2M/oauth?success=1


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








