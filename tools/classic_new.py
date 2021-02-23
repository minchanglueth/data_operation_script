from google_spreadsheet_api.function import update_value
from google_spreadsheet_api.function import get_df_from_speadsheet, get_gsheet_name
from core.crud.get_df_from_query import get_df_from_query
from core.crud.sql.user import get_user_uuid_from_user_name
from core.crud.sql.artist import get_uuid_and_count_from_artist_name
from core.crud.sql.genre import get_genre_uuid_from_genre_name
from core.crud.sql.crawlingtask import get_crawl_E5_06_status, get_datasourceId_from_crawlingtask
from core.crud.sql.external_identity import get_trackid_from_ituneid_and_tracknum
from youtube_dl_fuction.fuctions import get_youtube_title_and_youtube_uploader_from_youtube_url
from tools.crawlingtask import crawl_youtube, crawl_itunes_album, sheet_type

from support_function.text_similarity.text_similarity import get_token_set_ratio

from tools.get_uuid4 import get_uuid4
import time
import pandas as pd
from core import query_path
from numpy import random
import numpy as np


#def crawl_itunes_album():
def similarity():
    special_characters = \
    get_df_from_speadsheet(gsheet_id='1W1TlNDXqZTMAaAFofrorqaEo6bfX7GjwnhWMXcq70xA', sheet_name='Similarity')[
        'Keywords'].tolist()
    row_index = df.index
    list = []
    for i in row_index:
        youtube_url = df['url_to_add'].loc[i]
        track_title = df['Song Title on Itunes'].loc[i].lower()
        get_youtube_info = get_youtube_title_and_youtube_uploader_from_youtube_url(youtube_url)
        get_youtube_title = get_youtube_info['youtube_title'].lower()
        get_youtube_uploader = get_youtube_info['uploader'].lower()
        get_youtube_duration = get_youtube_info['duration']
        print(f"{track_title}----{get_youtube_title}")

        result = "type 3"
        for special_character in special_characters:
            if special_character in track_title:
                result = "type 1"
                break
            elif special_character in get_youtube_title:
                result = "type 2"
                break
            else:
                pass
        if result == "type 1":
            if special_character in get_youtube_title:
                token_set_ratio = get_token_set_ratio(get_youtube_title, track_title)
            else:
                token_set_ratio = 0
        elif result == "type 2":
            if special_character in track_title:
                token_set_ratio = get_token_set_ratio(get_youtube_title, track_title)
            else:
                token_set_ratio = 0
        else:
            token_set_ratio = get_token_set_ratio(get_youtube_title, track_title)

        print(f"{token_set_ratio}-----{result}-----{special_character}-----{track_title}-----{get_youtube_title}")

        list.extend([get_youtube_title, get_youtube_uploader, get_youtube_duration, token_set_ratio])

    data_frame = pd.DataFrame(np.array(list).reshape(-1, 4),
                              columns=["youtube_title", "youtube_uploader", "duration", "token_set_ratio"])

    print(data_frame)
    updated_df = data_frame
    # print(updated_df)
    # column_name = ["youtube_title", "youtube_uploader", "token_set_ratio"]
    # list_result = updated_df.values.tolist()  # transfer data_frame to 2D list
    # list_result.insert(0, column_name)
    # range_to_update = f"{sheet_name}!J1"
    # update_value(list_result, range_to_update, gsheet_id)  # validate_value type: object, int, category... NOT DATETIME


if __name__ == "__main__":
    # https://docs.google.com/spreadsheets/d/1aRhZ7NQAfhud3jjR5aboCZ3Ew8u2Y0SqGqUQYwcUnBs/edit#gid=98817891
    start_time = time.time()
    pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)
    gsheet_id = '1aRhZ7NQAfhud3jjR5aboCZ3Ew8u2Y0SqGqUQYwcUnBs'
    gsheet_name = get_gsheet_name(gsheet_id=gsheet_id)
    sheet_name = 'MP_3'
    df = get_df_from_speadsheet(gsheet_id=gsheet_id, sheet_name= sheet_name)
    df = df.head(10)
    similarity()

    # print(k)
    # sheet_info = sheet_type.MP3_SHEET_NAME
    # Start tools:

    print("--- %s seconds ---" % (time.time() - start_time))
