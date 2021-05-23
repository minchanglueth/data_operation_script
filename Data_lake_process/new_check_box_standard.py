from colorama import Fore, Style

from Data_lake_process.class_definition import WhenExist, PageType, SheetNames, merge_file, Page, DataReports, \
    get_key_value_from_gsheet_info, add_key_value_from_gsheet_info, get_gsheet_id_from_url

from crawl_itune.functions import get_max_ratio, check_validate_itune, get_itune_id_region_from_itune_url
from google_spreadsheet_api.function import update_value


def youtube_check_box(page_name: str, df: object, sheet_name: str):
    df['len'] = df['url_to_add'].apply(lambda x: len(x))
    if page_name in ("TopSingle", "TopAlbum") and sheet_name == SheetNames.MP3_SHEET_NAME:

        youtube_check_box = df[~
        ((
                 (df['track_id'] != '')
                 & (df['memo'] == 'added')
                 & (df['len'] == 43)
                 & (df['type'].isin(["c", "d", "z"]))
                 & (df['checking_mp3'] == 'TRUE')
                 & (df['already_existed'] == 'null')
         ) |
         (
                 (df['track_id'] != '')
                 & (df['memo'] == '')
                 & (df['url_to_add'] == '')
                 & (df['type'] == '')
                 & ~((df['checking_mp3'] == 'TRUE')
                     & (df['already_existed'] == 'null'))
         ) |
         (
                 (df['track_id'] != '')
                 & (df['memo'] == 'not found')
                 & (df['len'] == 0)
                 & (df['checking_mp3'] == 'TRUE')
                 & (df['already_existed'] == 'null')
         )
         )
        ]
    elif page_name == "NewClassic" and sheet_name == SheetNames.MP3_SHEET_NAME:
        youtube_check_box = df[~
        ((
                 (df['track_id'] != '')
                 & (df['memo'] == 'added')
                 & (df['len'] == 43)
                 & (df['type'].isin(["c", "d", "z"]))
                 & (df['checking_mp3'] == 'TRUE')
                 & (df['is_released'] == 'TRUE')
         ) |
         (
                 (df['track_id'] != '')
                 & (df['memo'] == '')
                 & (df['url_to_add'] == '')
                 & (df['type'] == '')
                 & ~((df['checking_mp3'] == 'TRUE')
                     & (df['is_released'] == 'TRUE'))
         ) |
         (
                 (df['track_id'] != '')
                 & (df['memo'] == 'not found')
                 & (df['len'] == 0)
                 & (df['checking_mp3'] == 'TRUE')
                 & (df['is_released'] == 'TRUE')
         )
         )
        ]
    elif page_name == "TopSingle" and sheet_name == SheetNames.MP4_SHEET_NAME:

        youtube_check_box = df[~
        ((
                 (df['track_id'] != '')
                 & (df['memo'] == 'added')
                 & (df['len'] == 43)
                 & (df['checking_mp4'] == 'TRUE')
         ) |
         (
                 (df['track_id'] != '')
                 & (df['memo'] == '')
                 & (df['url_to_add'] == '')
                 & ~((df['checking_mp4'] == 'TRUE')
         )
         ) |
         (
                 (df['track_id'] != '')
                 & (df['memo'] == 'not found')
                 & (df['len'] == 0)
                 & (df['checking_mp4'] == 'TRUE')
         )
         )
        ]
    elif page_name == "NewClassic" and sheet_name == SheetNames.MP4_SHEET_NAME:
        youtube_check_box = df[~
        ((
                 (df['track_id'] != '')
                 & (df['memo'] == 'added')
                 & (df['len'] == 43)
                 & (df['checking_mp4'] == 'TRUE')
                 & (df['is_released'] == 'TRUE')
                 & (df['verified'] == 'null')
         ) |
         (
                 (df['track_id'] != '')
                 & (df['memo'] == '')
                 & (df['url_to_add'] == '')
                 & ~(
                 (df['checking_mp4'] == 'TRUE')
                 & (df['is_released'] == 'TRUE')
                 & (df['is_released'] == 'null')
         )
         ) |
         (
                 (df['track_id'] != '')
                 & (df['memo'] == 'not found')
                 & (df['len'] == 0)
                 & (df['checking_mp4'] == 'TRUE')
                 & (df['is_released'] == 'TRUE')
                 & (df['verified'] == 'null')
         )
         )
        ]
    elif page_name == "TopAlbum" and sheet_name == SheetNames.MP4_SHEET_NAME:
        youtube_check_box = df[~
        ((
                 (df['track_id'] != '')
                 & (df['memo'] == 'added')
                 & (df['len'] == 43)
                 & (df['checking_mp4'] == 'TRUE')
                 & (df['already_existed'] == 'null')
                 & (df['verified'] == 'null')
         ) |
         (
                 (df['track_id'] != '')
                 & (df['memo'] == '')
                 & (df['url_to_add'] == '')
                 & ~(
                 (df['checking_mp4'] == 'TRUE')
                 & (df['already_existed'] == 'null')
                 & (df['verified'] == 'null')
         )
         ) |
         (
                 (df['track_id'] != '')
                 & (df['memo'] == 'not found')
                 & (df['len'] == 0)
                 & (df['checking_mp4'] == 'TRUE')
                 & (df['already_existed'] == 'null')
                 & (df['verified'] == 'null')
         )
         )
        ]
    if youtube_check_box.empty:
        print(Fore.LIGHTYELLOW_EX + f"Pass check box" + Style.RESET_ALL)
        return True
    else:
        print(youtube_check_box)
        return False


def s11_checkbox(df: object, page_type: str = None):
    df['url'] = df['gsheet_info'].apply(
        lambda x: get_key_value_from_gsheet_info(gsheet_info=x, key='url'))
    # Step 1: check validate format
    check_format_s11 = df[~((df['itune_album_url'] == 'not found') | (
            df['itune_album_url'].str[:32] == 'https://music.apple.com/us/album'))]
    if check_format_s11.empty:
        print(Fore.LIGHTYELLOW_EX + f"Pass check box" + Style.RESET_ALL)
        return True
    elif page_type == "contribution":
        print(Fore.LIGHTYELLOW_EX + f"Pass check box" + Style.RESET_ALL)
        return True
    else:
        print(Fore.LIGHTYELLOW_EX + f"Not pass check box" + Style.RESET_ALL)
        print(check_format_s11.head(10))
        return False


def update_s11_check_box(df: object):
    gsheet_infos = list(set(df.gsheet_info.tolist()))
    df['url'] = df['gsheet_info'].apply(
        lambda x: get_key_value_from_gsheet_info(gsheet_info=x, key='url'))

    df['itune_id'] = df['itune_album_url'].apply(
        lambda x: get_itune_id_region_from_itune_url(url=x)[0] if x != 'not found' else 'None')
    df['region'] = df['itune_album_url'].apply(
        lambda x: get_itune_id_region_from_itune_url(url=x)[1] if x != 'not found' else 'None')
    df['checking_validate_itune'] = df['itune_id'].apply(
        lambda x: check_validate_itune(x) if x != 'None' else 'None')
    df['token_set_ratio'] = df.apply(
        lambda x: get_max_ratio(itune_album_id=x['itune_id'],
                                input_album_title=x['album_title']) if x['itune_id'] != 'None' else 'None',
        axis=1)
    # Update data
    for gsheet_info in gsheet_infos:
        url = get_key_value_from_gsheet_info(gsheet_info=gsheet_info, key='url')
        df_to_upload = df[df['url'] == url].reset_index()
        # print(df_to_upload)
        column_name = ['itune_id', 'region', 'checking_validate_itune', 'token_set_ratio']
        updated_df = df_to_upload[column_name]

        list_result = updated_df.values.tolist()  # transfer data_frame to 2D list
        list_result.insert(0, column_name)
        range_to_update = f"{SheetNames.S_11}!Q1"
        update_value(list_result, range_to_update,
                     get_gsheet_id_from_url(url))  # validate_value type: object, int, category... NOT DATETIME

