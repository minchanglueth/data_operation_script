from core.models.data_source_format_master import DataSourceFormatMaster
from google_spreadsheet_api.function import (
    get_gsheet_name,
    get_list_of_sheet_title,
    get_df_from_speadsheet,
)
import time
import pandas as pd
from termcolor import colored
import json


def get_gsheet_id_from_url(url: object) -> object:
    url_list = url.split("/")
    gsheet_id = url_list[5]
    return gsheet_id


def get_key_value_from_gsheet_info(gsheet_info: str, key: str):
    value = json.loads(gsheet_info)[key]
    return value


def add_key_value_from_gsheet_info(gsheet_info: str, key_value: dict):
    sheet_info_log_ = json.loads(gsheet_info)
    for key, value in key_value.items():
        sheet_info_log_[key] = value
    sheet_info_log = json.dumps(sheet_info_log_)
    return sheet_info_log


class WhenExist:
    SKIP = "skip"
    REPLACE = "replace"
    KEEP_BOTH = "keep both"


class ObjectType:
    ARTIST = "artist"
    ALBUM = "album"
    TRACK = "track"


class DataReports:
    column_name = [
        "gsheet_name",
        "gsheet_url",
        "type",
        "running_time",
        "status",
        "count_complete",
        "count_incomplete",
        "notice",
    ]
    status_type_processing = "processing"
    status_type_done = "done"


class SheetNames:
    MP3_SHEET_NAME = "MP3_SHEET_NAME"
    MP4_SHEET_NAME = "MP4_SHEET_NAME"
    VERSION_SHEET_NAME = "VERSION_SHEET_NAME"
    ARTIST_IMAGE = "ARTIST_IMAGE"
    ALBUM_IMAGE = "ALBUM_IMAGE"
    ARTIST_WIKI = "ARTIST_WIKI"
    ALBUM_WIKI = "ALBUM_WIKI"
    TRACK_WIKI = "TRACK_WIKI"
    S_11 = "S_11"
    C_11 = "C_11"


class PageType:
    class NewClassic:
        name = "NewClassic"
        priority = 530

    class TopSingle:
        name = "TopSingle"
        priority = 530

    class TopAlbum:
        name = "TopAlbum"
        priority = 100

    class Contribution:
        name = "contribution"
        priority = 520

    class ArtistPage:
        name = "ArtistPage"
        priority = 80


class Page(object):
    def __init__(self, url: str, action: object = None):
        self.gsheet_id = get_gsheet_id_from_url(url=url)
        self.url = url
        self.action = action
        self.page_name = get_gsheet_name(gsheet_id=self.gsheet_id)
        self.sheet_name_type = self.SheetNameType(url=self.url, action=self.action )

    class SheetNameType:
        def __init__(self, url: str,  action: object = None):
            gsheet_id = get_gsheet_id_from_url(url=url)
            sheet_names = get_list_of_sheet_title(gsheet_id=gsheet_id)
            if "MP_3" in sheet_names:
                if action == 'crawling':
                    self.MP3_SHEET_NAME = {
                        "sheet_name": "MP_3",
                        "fomatid": DataSourceFormatMaster.FORMAT_ID_MP3_FULL,
                        "column_name": [
                            "track_id",
                            "memo",
                            "mp3_link",
                            "url_to_add",
                            "type",
                            "checking_mp3",
                            "already_existed",
                            "is_released",
                            "assignee",
                            "crawlingtask_id",
                        ],
                    }
                else:
                    self.MP3_SHEET_NAME = {
                        "sheet_name": "MP_3",
                        "fomatid": DataSourceFormatMaster.FORMAT_ID_MP3_FULL,
                        "column_name": [
                            "track_id",
                            "memo",
                            "mp3_link",
                            "url_to_add",
                            "type",
                            "checking_mp3",
                            "already_existed",
                            "is_released",
                            "assignee",
                        ],
                    }
            if "MP_4" in sheet_names:
                if action == 'crawling':
                    self.MP4_SHEET_NAME = {
                        "sheet_name": "MP_4",
                        "fomatid": DataSourceFormatMaster.FORMAT_ID_MP4_FULL,
                        "column_name": [
                            "track_id",
                            "memo",
                            "mp4_link",
                            "url_to_add",
                            "checking_mp4",
                            "already_existed",
                            "is_released",
                            "verified",
                            "assignee",
                            "crawlingtask_id",
                        ],
                    }
                else:
                    self.MP4_SHEET_NAME = {
                        "sheet_name": "MP_4",
                        "fomatid": DataSourceFormatMaster.FORMAT_ID_MP4_FULL,
                        "column_name": [
                            "track_id",
                            "memo",
                            "mp4_link",
                            "url_to_add",
                            "checking_mp4",
                            "already_existed",
                            "is_released",
                            "verified",
                            "assignee",
                        ],
                    }
            if "Version_done" in sheet_names:
                self.VERSION_SHEET_NAME = {
                    "sheet_name": "Version_done",
                    "fomatid": [
                        DataSourceFormatMaster.FORMAT_ID_MP4_REMIX,
                        DataSourceFormatMaster.FORMAT_ID_MP4_LIVE,
                    ],
                    "column_name": [
                        "track_id",
                        "remix_url",
                        "remix_artist",
                        "live_url",
                        "live_venue",
                        "live_year",
                    ],
                }
            if f"{SheetNames.ARTIST_IMAGE} cant upload" in sheet_names:
                if get_df_from_speadsheet(
                    gsheet_id, f"{SheetNames.ARTIST_IMAGE} cant upload"
                ).values.tolist() == [["Upload thành công 100% nhé các em ^ - ^"]]:
                    pass
                else:
                    self.ARTIST_IMAGE = {
                        "sheet_name": f"{SheetNames.ARTIST_IMAGE} cant upload",
                        "column_name": ["uuid", "memo", "url_to_add"],
                        "object_type": ObjectType.ARTIST,
                    }
            elif "image" in sheet_names:
                self.ARTIST_IMAGE = {
                    "sheet_name": "image",
                    "column_name": ["uuid", "memo", "url_to_add"],
                    "object_type": ObjectType.ARTIST,
                }
            elif "Artist_image" in sheet_names:
                self.ARTIST_IMAGE = {
                    "sheet_name": "Artist_image",
                    "column_name": ["uuid", "memo", "url_to_add"],
                    "object_type": ObjectType.ARTIST,
                }
            else:
                pass
            if f"{SheetNames.ALBUM_IMAGE} cant upload" in sheet_names:
                if get_df_from_speadsheet(
                    gsheet_id, f"{SheetNames.ALBUM_IMAGE} cant upload"
                ).values.tolist() == [["Upload thành công 100% nhé các em ^ - ^"]]:
                    pass
                else:
                    self.ALBUM_IMAGE = {
                        "sheet_name": f"{SheetNames.ALBUM_IMAGE} cant upload",
                        "column_name": ["uuid", "memo", "url_to_add"],
                        "object_type": ObjectType.ALBUM,
                    }
            elif "Album_image" in sheet_names:
                self.ALBUM_IMAGE = {
                    "sheet_name": "Album_image",
                    "column_name": ["uuid", "memo", "url_to_add"],
                    "object_type": ObjectType.ALBUM,
                }
            else:
                pass
            if "Artist_wiki" in sheet_names:
                self.ARTIST_WIKI = {
                    "sheet_name": "Artist_wiki",
                    "column_name": ["uuid", "memo", "url_to_add", "content_to_add"],
                    "table_name": "artists",
                }
            if "Album_wiki" in sheet_names:
                self.ALBUM_WIKI = {
                    "sheet_name": "Album_wiki",
                    "column_name": ["uuid", "memo", "url_to_add", "content_to_add"],
                    "table_name": "albums",
                }
            if "Track_wiki" in sheet_names:
                self.TRACK_WIKI = {
                    "sheet_name": "Track_wiki",
                    "column_name": ["id", "memo", "url_to_add", "content_to_add"],
                    "table_name": "tracks",
                }
            if "S_11" in sheet_names:
                self.S_11 = {
                    "sheet_name": "S_11",
                    "column_name": [
                        "release_date",
                        "album_title",
                        "album_artist",
                        "itune_album_url",
                        "sportify_album_url",
                    ],
                }
            if "Youtube collect_experiment" in sheet_names:
                self.C_11 = {
                    "sheet_name": "Youtube collect_experiment",
                    "column_name": [
                        "pre_valid",
                        "p.i.c",
                        "itune_album_url",
                        "official_music_video_2",
                        "artist_name",
                        "year",
                        "live_concert_name_place",
                        "track_title/track_num",
                        "contribution_link",
                        "content type",
                        "pointlogsid",
                        "itune_id",
                        "region",
                        "checking_validate_itune",
                        "06_id",
                        "06_status",
                        "e5_id",
                        "e5_status",
                        "track_title",
                        "track_id",
                        "similarity",
                        "recheck",
                        "d9_id",
                        "d9_status",
                    ],
                }

    def process_file(self, sheet_info: str):
        sheet_name = sheet_info.get("sheet_name")
        column_names = sheet_info.get("column_name")
        df = get_df_from_speadsheet(gsheet_id=self.gsheet_id, sheet_name=sheet_name)

        lower_names = [name.lower() for name in df.columns]
        df.columns = lower_names

        if sheet_name in get_list_of_sheet_title(gsheet_id=self.gsheet_id):
            # reformat_column_name
            df.columns = df.columns.str.replace("track_id", "track_id")
            df.columns = df.columns.str.replace("track id", "track_id")
            df.columns = df.columns.str.replace("trackid", "track_id")

            df.columns = df.columns.str.replace("s12", "memo")
            df.columns = df.columns.str.replace("a12", "memo")

            df.columns = df.columns.str.replace("mp3_link", "mp3_link")
            df.columns = df.columns.str.replace("mp3link", "mp3_link")
            df.columns = df.columns.str.replace("mp4_link", "mp4_link")
            df.columns = df.columns.str.replace("mp4link", "mp4_link")

            df.columns = df.columns.str.replace("url to add", "url_to_add")
            df.columns = df.columns.str.replace("artist_url_to_add", "url_to_add")

            df.columns = df.columns.str.replace("artist_uuid", "uuid")
            df.columns = df.columns.str.replace("objectid", "uuid")
            df.columns = df.columns.str.replace("album_uuid", "uuid")

            df.columns = df.columns.str.replace("content tomadd", "content_to_add")

            df.columns = df.columns.str.replace("albumtitle", "album_title")
            df.columns = df.columns.str.replace("albumartist", "album_artist")
            df.columns = df.columns.str.replace("itunes_album_url", "itune_album_url")
            df.columns = df.columns.str.replace("itunes_album_link", "itune_album_url")
            df.columns = df.columns.str.replace("albumurl", "sportify_album_url")

            df_columns = df.columns
            column_name_reformat = []
            for column_name in column_names:
                if column_name in df_columns:
                    column_name_reformat.append(column_name)
            df = df[column_name_reformat]
            return df
        else:
            print(f"sheet_name: {sheet_name} not have")

    def media_file(self, sheet_info: object, page_priority: int):
        df = self.process_file(sheet_info=sheet_info)
        info = {
            "url": f"{self.url}",
            "gsheet_id": f"{self.gsheet_id}",
            "gsheet_name": f"{get_gsheet_name(gsheet_id=self.gsheet_id)}",
            "sheet_name": f"{sheet_info.get('sheet_name')}",
            # "page_type": "top_single",
            "page_priority": page_priority,
        }
        df["gsheet_info"] = df.apply(lambda x: json.dumps(info), axis=1)
        return df


# media_file = Page.media_file
# SheetNameType = Page.SheetNameType


def merge_file(sheet_name: str, urls: list, page_type: object = None, action: object = None):
    # Step 1: remove duplicate url
    url_reformats = list(
        set(
            map(
                lambda x: "https://docs.google.com/spreadsheets/d/"
                + get_gsheet_id_from_url(x),
                urls,
            )
        )
    )
    # print(url_reformats)
    priority = page_type.priority
    df = pd.DataFrame()
    for url in url_reformats:
        try:
            if action == 'crawling':
                page = Page(url=url,action="crawling")
            else:
                page = Page(url=url)
            sheet_info = getattr(page.sheet_name_type, sheet_name)
            df_ = page.media_file(sheet_info=sheet_info, page_priority=priority)
            df = df.append(df_, ignore_index=True)
        except AttributeError:
            print(
                colored(
                    f"AttributeError: {page.page_name} object has no attribute {sheet_name}\nurl: {url}",
                    "yellow",
                )
            )
    return df


if __name__ == "__main__":
    start_time = time.time()
    pd.set_option(
        "display.max_rows", None, "display.max_columns", 50, "display.width", 1000
    )
    urls = [
        # "https://docs.google.com/spreadsheets/d/1byh3LFm1NhzFPLfROyS9kwgqKKG2P8k7aTnGQXAXr-o/edit#gid=1962142102",
        "https://docs.google.com/spreadsheets/d/1zAgHdr0DaHjfNdf8_IkmogQF69vKKBQv51Ay2hPy-D4/edit#gid=472445320",
    ]

    sheet_name = SheetNames.ARTIST_IMAGE
    page_type = PageType.ArtistPage
    k = merge_file(sheet_name=sheet_name, urls=urls, page_type=page_type)
    print(k.head(10))

    # get_df_from_speadsheet(gsheet_id="1ZgMTydySAvqoyJgo4OZchQVC42NZgHbzocdy50IH2LY", sheet_name="S_11")

    print("\n --- total time to process %s seconds ---" % (time.time() - start_time))
