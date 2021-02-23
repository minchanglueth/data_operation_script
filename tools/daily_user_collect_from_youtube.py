from core.crud.sql.pointlog import collect_from_youtube_query
from core.crud.get_df_from_query import get_df_from_query
from datetime import date, timedelta
from youtube_dl_fuction.fuctions import get_youtube_title_and_youtube_uploader_from_youtube_url
from google_spreadsheet_api.function import get_list_of_sheet_title, delete_sheet
import time
import pandas as pd
import re, datetime
from numpy import random

from google_spreadsheet_api.create_new_sheet_and_update_data_from_df import creat_new_sheet_and_update_data_from_df


def get_date_from_str(str: str):
    match = re.search('\d{4}-\d{2}-\d{2}', str)
    date = datetime.datetime.strptime(match.group(), '%Y-%m-%d').date()
    return date


def daily_user_collect_from_youtube():
    # INPUT HERE:
    # Input_url 'https://docs.google.com/spreadsheets/d/1vlMsEjwBWuuxXecadJsEbBFeuVFAHZSbOz90JhXgioo/edit#gid=1088561556'
    gsheet_id = '1vlMsEjwBWuuxXecadJsEbBFeuVFAHZSbOz90JhXgioo'
    sheet_name = 'Sheet1'
    new_title = f"Daily contribution {date.today()}"
    print(new_title)

    # PROCESS HERE:
    # STEP 1: Get data

    pd.set_option("display.max_rows", None, "display.max_columns", 30, 'display.width', 1000)
    start_time1 = time.time()
    df = get_df_from_query(collect_from_youtube_query())
    df = df.fillna(value='None').astype({"created_at": 'str'})
    df['contribution_url'] = df['contribution_url'].apply(lambda x: x.replace('"', ""))
    # df = df.head(10)

    row_index = df.index
    get_youtube_titles = []
    get_youtube_uploaders = []
    get_youtube_durations = []
    for i in row_index:

        youtube_url = df.contribution_url.loc[i]

        get_youtube_info = get_youtube_title_and_youtube_uploader_from_youtube_url(youtube_url)
        get_youtube_title = get_youtube_info['youtube_title']
        get_youtube_uploader = get_youtube_info['uploader']
        get_youtube_duration = get_youtube_info['duration']

        get_youtube_titles.append(get_youtube_title)
        get_youtube_uploaders.append(get_youtube_uploader)
        get_youtube_durations.append(get_youtube_duration)

    se_youtube_title = pd.Series(get_youtube_titles)
    se_youtube_uploader = pd.Series(get_youtube_uploaders)
    se_youtube_duration = pd.Series(get_youtube_durations)
    df['get_youtube_title'] = se_youtube_title.values
    df['get_youtube_uploader'] = se_youtube_uploader.values
    df['se_youtube_duration'] = se_youtube_duration.values
    print(df)

    # print("\n", "Get data result \n", df)
    # STEP 2: Create sheet and update data to sheet
    creat_new_sheet_and_update_data_from_df(df, gsheet_id, new_title)


    # STEP 3: delete sheet_name > 7 days ago:
    for sheet_name in get_list_of_sheet_title(gsheet_id=gsheet_id):
        sheet_date = get_date_from_str(sheet_name)
        if sheet_date > date.today() - timedelta(days=9):
            print(f"Keep sheet_name: {sheet_name}: {sheet_date}")

        else:
            print(f"delete sheet_name: {sheet_name}: {sheet_date}")
            delete_sheet(gsheet_id=gsheet_id, sheet_name=sheet_name)

    print("\n --- %s seconds ---" % (time.time() - start_time1))


if __name__ == "__main__":
    daily_user_collect_from_youtube()
    # str = "I have a meeting on 2018-12-10 in New York"
    # k = get_date_from_str(str)
    # print(k)





    # joy xinh
