from google_spreadsheet_api.function import update_value
from google_spreadsheet_api.function import get_df_from_speadsheet
from core.crud.get_df_from_query import get_df_from_query

from youtube_dl_fuction.fuctions import get_raw_title_uploader_from_youtube_url

from support_function.text_similarity.text_similarity import get_token_set_ratio

import time
import pandas as pd
from core import query_path
from numpy import random
import numpy as np


def intern_checking_process():
    df = joy
    # df = joy.head(10)
    row_index = df.index
    list = []

    for i in row_index:
        youtube_url = df['url_to_add'].loc[i]
        status = df['url_to_add'].loc[i]
        track_title = df['TrackTitle'].loc[i]
        if status != "":
            get_youtube_info = get_raw_title_uploader_from_youtube_url(youtube_url)
            get_youtube_title = get_youtube_info['youtube_title']
            get_youtube_uploader = get_youtube_info['uploader']
            token_set_ratio = get_token_set_ratio(get_youtube_title, track_title)

        else:

            get_youtube_title = "not to check"
            get_youtube_uploader = "not to check"
            token_set_ratio = "not to check"
        list.extend([get_youtube_title, get_youtube_uploader, token_set_ratio])

    data_frame = pd.DataFrame(np.array(list).reshape(-1, 3),
                              columns=["youtube_title", "youtube_uploader", "token_set_ratio"])
    print(data_frame)

    updated_df = data_frame
    column_name = ["youtube_title", "youtube_uploader", "token_set_ratio"]
    list_result = updated_df.values.tolist()  # transfer data_frame to 2D list
    list_result.insert(0, column_name)
    range_to_update = f"{sheet_name}!AG1"
    update_value(list_result, range_to_update, gsheet_id)  # validate_value type: object, int, category... NOT DATETIME


if __name__ == "__main__":
    start_time = time.time()

    pd.set_option("display.max_rows", None, "display.max_columns", 80, 'display.width', 1000)
    start_time = time.time()
    # INPUT HERE:
    # Input_url 'https://docs.google.com/spreadsheets/d/1ACU3JdsJ80PavxJShHsIsJxGtPE054l0hN5JrzV6GtE/edit#gid=1365208059'
    gsheet_id = '1ACU3JdsJ80PavxJShHsIsJxGtPE054l0hN5JrzV6GtE'  # Single page
    sheet_name = 'MP_4'
    joy = get_df_from_speadsheet(gsheet_id, sheet_name).fillna(value='None').apply(lambda x: x.str.strip())

    # PROCESS HERE:
    intern_checking_process()
    print("--- %s seconds ---" % (time.time() - start_time))
