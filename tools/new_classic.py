import time
import pandas as pd
from crawl_itune.functions import get_max_ratio, check_validate_itune, get_itune_id_region_from_itune_url
from google_spreadsheet_api.function import get_df_from_speadsheet, get_list_of_sheet_title
from Data_lake_process.crawlingtask import sheet_type
from Data_lake_process.data_lake_standard import process_S_11
import json


def check_validate():
    original_df['itune_id'] = original_df['Itunes_Album_URL'].apply(
        lambda x: get_itune_id_region_from_itune_url(url=x)[0])
    original_df['region'] = original_df['Itunes_Album_URL'].apply(
        lambda x: get_itune_id_region_from_itune_url(url=x)[1])
    original_df['checking_validate_itune'] = original_df['itune_id'].apply(lambda x: check_validate_itune(x))
    original_df['token_set_ratio'] = original_df.apply(
        lambda x: get_max_ratio(itune_album_id=x['itune_id'], input_album_title=x.AlbumTitle), axis=1)
    print(original_df)
    # check_original_df = original_df[(original_df['checking_validate_itune'] != True)]
    # return check_original_df.checking_validate_itune


def check_youtube_url_mp3(gsheet_id: str, sheet_info: object):
    '''
    MP3_SHEET_NAME = {"sheet_name": "MP_3", "fomatid": DataSourceFormatMaster.FORMAT_ID_MP3_FULL,
                      "column_name": ["track_id", "Memo", "Mp3_link", "url_to_add"]}
    '''

    sheet_name = 'MP_3'
    original_df = get_df_from_speadsheet(gsheet_id, sheet_name).applymap(str.lower)
    original_df.columns = original_df.columns.str.replace('TrackId', 'track_id')
    original_df.columns = original_df.columns.str.replace('MP3_link', 'Mp3_link')

    original_df = get_df_from_speadsheet(gsheet_id, sheet_name).applymap(str.lower)[
        sheet_info.get('column_name')]




    # original_df['len'] = original_df['url_to_add'].apply(lambda x: len(x))
    # youtube_url_mp3 = original_df[['track_id', 'Memo', 'url_to_add', 'len', 'Type', 'Assignee']]
    #
    # check_youtube_url_mp3 = youtube_url_mp3[~
    # ((
    #          (youtube_url_mp3['track_id'] != '')
    #          & (youtube_url_mp3['Memo'] == 'added')
    #          & (youtube_url_mp3['len'] == 43)
    #          & (youtube_url_mp3['Type'].isin(["c", "d", "z"]))
    #  ) |
    #  (
    #          (youtube_url_mp3['track_id'] != '')
    #          & (youtube_url_mp3['Memo'] == 'not found')
    #          & (youtube_url_mp3['url_to_add'] == 'none')
    #          & (youtube_url_mp3['Type'] == 'none')
    #  ) |
    #  (
    #
    #      (youtube_url_mp3['Assignee'] == 'no need to check')
    #  ))
    # ]

    # return check_youtube_url_mp3.track_id.str.upper()
if __name__ == "__main__":
    start_time = time.time()
    pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)
    urls = [
        "https://docs.google.com/spreadsheets/d/1W2QmYccbfeEAOEboKGSFWhv9hsXoQGPSZUhMP9Njsfw/edit#gid=1941765562",
        "https://docs.google.com/spreadsheets/d/15LL8rcVnsWjE7D4RvrIMRpH8Y9Lgyio-kcs4mE540MI/edit#gid=1308575784"
    ]
    sheet_info = sheet_type.S_11
    joy = process_S_11(urls=urls, sheet_info=sheet_info)
    k = check_validate(df=joy)

    # k = check_youtube_url_mp3(gsheet_id="1W2QmYccbfeEAOEboKGSFWhv9hsXoQGPSZUhMP9Njsfw", sheet_info=sheet_info)
    print("--- %s seconds ---" % (time.time() - start_time))
