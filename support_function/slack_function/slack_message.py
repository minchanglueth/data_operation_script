# from create_report_topalbum import create_update_spreadsheet
from slack_sdk.errors import SlackApiError
from support_function.slack_function.slack_connect import client_slack

maa_crawl_not_itunes = """Hi <@U01E6SP58HK|cal> !
CC: <@UDW03RVGR|cal>
User Contribution on {}: 
{} request(s) have been found and added to DB by data team!"""
# nhớ chỉnh tag name nha

cy_notItunes_extract = """Hi <@U01E6SP58HK|cal> !
CC: <@UDW03RVGR|cal>
User Contribution on {}: 
{} missing song(s) not from itunes are ready for further checking!"""

cy_notItunes_plupdate = """Hi <@U01E6SP58HK|cal> !
CC: <@UDW03RVGR|cal>
User Contribution on {}: 
{} missing song(s) not from itunes were checked/added to vibbidi single pages!"""

ma_itunes_plupdate = """Hi <@U01E6SP58HK|cal> !
CC: <@UDW03RVGR|cal>
User Contribution on {}: 
{} artist(s) found from itunes were added/updated with album/tracklists for further checking!"""

maa_itunes_plupdate = """Hi <@U01E6SP58HK|cal> !
CC: <@UDW03RVGR|cal>
User Contribution on {}: 
{} album(s) found from itunes were added/updated with tracklists for further checking!"""

cy_Itunes_plupdate = """Hi <@U01E6SP58HK|cal> !
CC: <@UDW03RVGR|cal>
User Contribution on {} checked on {}: 
{} missing song(s) whose tracklists found from itunes were checked/added successfully to vibbidi single pages!"""

class send_message_slack:
    def __init__(self, actiontype_description, count_id, message_type, date):
        self.actiontype_description = actiontype_description
        self.count_id = count_id
        self.message_type = message_type
        self.date = date

    def msg_slack(self):
        # topalbum_crawler = message
        report_crawler_updated = self.message_type.format(self.actiontype_description, self.date, self.count_id)
        return report_crawler_updated
    
    def send_to_slack(self):
        report_crawler_updated = self.msg_slack()
        print(report_crawler_updated)
        # report_crawler_updated = self.message_type.format(self.actiontype_description, self.count_id, self.date)
        try:
            # client_slack.chat_postMessage(channel='minchan-testing', text=str(report_crawler_updated)) #MM<3
            client_slack.chat_postMessage(channel='unit-user-contribution', text=str(report_crawler_updated)) #vibbidi-correct
            #client_slack.chat_postMessage(channel='data-auto-error', text=str(report_crawler_updated)) #vibbidi-test
        except SlackApiError as e:
        ## You will get a SlackApiError if "ok" is False
            assert e.response["ok"] is False
            assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
            print(f"Got an error: {e.response['error']}")
            print(f"Got an error: {e.response['ok']}")

    def send_to_slack_error(self):
        report_crawler_updated = self.message_type.format(self.actiontype_description, self.count_id, self.date)
        try:
            client_slack.chat_postMessage(channel='minchan-error', text=str(report_crawler_updated)) #MM<3
            #client_slack.chat_postMessage(channel='data-auto-report', text=str(report_crawler_updated)) #vibbidi-correct
            #client_slack.chat_postMessage(channel='data-auto-error', text=str(report_crawler_updated)) #vibbidi-test
        except SlackApiError as e:
        ## You will get a SlackApiError if "ok" is False
            assert e.response["ok"] is False
            assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
            print(f"Got an error: {e.response['error']}")
            print(f"Got an error: {e.response['ok']}")
