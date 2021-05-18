
from colorama import Fore, Style

from Data_lake_process.class_definition import WhenExist, PageType, SheetNames, merge_file, Page, DataReports, \
    get_key_value_from_gsheet_info, add_key_value_from_gsheet_info, get_gsheet_id_from_url


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

    if youtube_check_box.empty:
        print(Fore.LIGHTYELLOW_EX + f"Pass check box" + Style.RESET_ALL)
        return True
    else:
        print(youtube_check_box)
        return False