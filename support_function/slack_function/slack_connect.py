# import logging

# logging.basicConfig(level=logging.DEBUG)
from slack_sdk import WebClient

# Get application first: https://api.slack.com/apps/A02341UQV2M/oauth?success=1

client_slack = WebClient(token='xoxb-1216139827667-1906835181267-xoLprkbWxRUG3Kv3xUVJ2iGO')  # slack ở bên M<3
# client_slack = WebClient(token='xoxb-1216139827667-1906835181267-DJ8Y1MyqU1UlJxSVXBRYr6xI') # slack ở bên M<3
# client_slack = WebClient(token='xoxb-3598555470-1887074714135-mtHVkOQia1xZXqI7QEt8yQWs') # slack ở VIBBIDI
# xoxb-3598555470-1887074714135-mtHVkOQia1xZXqI7QEt8yQWs
# xapp-1-A01T7HUJ4F2-1887112284519-553554ddb5acb55a80238ea6b19e472f9a0ec9b9857a3032ee10cff784ba2dc8

# # để DELETE slack post
# from slack_sdk.errors import SlackApiError
# import sys

# try:
#     # client_slack.chat_postMessage(channel='minchan-testing', text=str(report_crawler_updated)) # slack channel bên M<3
#     # client_slack.chat_postMessage(channel='data-auto-report', text=str(report_crawler_updated))
#     # client_slack.chat_postMessage(channel='data-internal', text=str(report_crawler_updated))
#     client_slack.chat_delete(channel='C3C7S8KLP',ts='1620275236.101200')
#     #client_slack.chat_delete(channel='C3C7S8KLP',ts='1618911613.083000')
# except SlackApiError as e:
# ## You will get a SlackApiError if "ok" is False
#     assert e.response["ok"] is False
#     assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
#     print(f"Got an error: {e.response['error']}")
#     print(f"Got an error: {e.response['ok']}")