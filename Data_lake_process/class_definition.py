from core.models.data_source_format_master import DataSourceFormatMaster
from google_spreadsheet_api.function import get_gsheet_name, get_list_of_sheet_title, get_df_from_speadsheet
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
    column_name = ["gsheet_name", "gsheet_url", "type", "running_time", "status", "count_complete", "count_incomplete",
                   "notice"]
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


class PageType:
    class NewClassic:
        name = "NewClassic"
        priority = 999

    class TopSingle:
        name = "TopSingle"
        priority = 19


class Page(object):
    def __init__(self, url: str):
        self.gsheet_id = get_gsheet_id_from_url(url=url)
        self.url = url
        self.page_name = get_gsheet_name(gsheet_id=self.gsheet_id)
        self.sheet_name_type = self.SheetNameType(url=self.url)

    class SheetNameType:
        def __init__(self, url: str):
            gsheet_id = get_gsheet_id_from_url(url=url)
            list_of_sheet_title = get_list_of_sheet_title(gsheet_id=gsheet_id)
            sheet_names = get_list_of_sheet_title(gsheet_id=gsheet_id)
            if "MP_3" in sheet_names:
                self.MP3_SHEET_NAME = {"sheet_name": "MP_3", "fomatid": DataSourceFormatMaster.FORMAT_ID_MP3_FULL,
                                       "column_name": ["track_id", "memo", "mp3_link", "url_to_add", "type"]}
            if "MP_4" in sheet_names:
                self.MP4_SHEET_NAME = {"sheet_name": "MP_4", "fomatid": DataSourceFormatMaster.FORMAT_ID_MP4_FULL,
                                       "column_name": ["track_id", "memo", "mp4_link", "url_to_add"]}
            if "Version_done" in sheet_names:
                self.VERSION_SHEET_NAME = {"sheet_name": "Version_done",
                                           "fomatid": [DataSourceFormatMaster.FORMAT_ID_MP4_REMIX,
                                                       DataSourceFormatMaster.FORMAT_ID_MP4_LIVE],
                                           "column_name": ["track_id", "remix_url", "remix_artist", "live_url",
                                                           "live_venue", "live_year"]}
            if f"{SheetNames.ARTIST_IMAGE} cant upload" in list_of_sheet_title:
                if get_df_from_speadsheet(gsheet_id, f"{SheetNames.ARTIST_IMAGE} cant upload").values.tolist() == [['Upload thành công 100% nhé các em ^ - ^']]:
                    pass
                else:
                    sheet_name = f"{SheetNames.ARTIST_IMAGE} cant upload"
                    self.ARTIST_IMAGE = {"sheet_name": f"{sheet_name}", "column_name": ["uuid", "memo", "url_to_add"],
                                         "object_type": ObjectType.ARTIST}
            elif "image" in list_of_sheet_title:
                sheet_name = "image"
                self.ARTIST_IMAGE = {"sheet_name": f"{sheet_name}", "column_name": ["uuid", "memo", "url_to_add"],
                                     "object_type": ObjectType.ARTIST}
            elif "Artist_image" in list_of_sheet_title:
                sheet_name = "Artist_image"
                self.ARTIST_IMAGE = {"sheet_name": f"{sheet_name}", "column_name": ["uuid", "memo", "url_to_add"],
                                     "object_type": ObjectType.ARTIST}
            else:
                pass
            if "Album_image" in sheet_names:
                self.ALBUM_IMAGE = {"sheet_name": "Album_image", "column_name": ["uuid", "memo", "url_to_add"],
                                    "object_type": ObjectType.ALBUM}
            if "Artist_wiki" in sheet_names:
                self.ARTIST_WIKI = {"sheet_name": "Artist_wiki",
                                    "column_name": ["uuid", "memo", "url_to_add", "content_to_add"],
                                    "table_name": "artists"}
            if "Album_wiki" in sheet_names:
                self.ALBUM_WIKI = {"sheet_name": "Album_wiki",
                                   "column_name": ["uuid", "memo", "url_to_add", "content_to_add"],
                                   "table_name": "albums"}
            if "Track_wiki" in sheet_names:
                self.TRACK_WIKI = {"sheet_name": "Track_wiki",
                                   "column_name": ["id", "memo", "url_to_add", "content_to_add"],
                                   "table_name": "tracks"}
            if "S_11" in sheet_names:
                self.S_11 = {"sheet_name": "S_11",
                             "column_name": ["release_date", "album_title", "album_artist", "itune_album_url",
                                             "sportify_album_url"]}

    def process_file(self, sheet_info: str):

        '''
        Why call object not show attribute: example: self.MP3_SHEET_NAME, self.MP4_SHEET_NAME
        VERSION_SHEET_NAME = {"sheet_name": "Version_done", "fomatid": [DataSourceFormatMaster.FORMAT_ID_MP4_REMIX,
                                                                DataSourceFormatMaster.FORMAT_ID_MP4_LIVE],
                      "column_name": ["track_id", "remix_url", "remix_artist", "live_url", "live_venue",
                                      "live_year"]}
        '''
        sheet_name = sheet_info.get('sheet_name')
        column_name = sheet_info.get('column_name')
        if sheet_name in get_list_of_sheet_title(gsheet_id=self.gsheet_id):
            df = get_df_from_speadsheet(gsheet_id=self.gsheet_id, sheet_name=sheet_name)
            # reformat_column_name
            df.columns = df.columns.str.replace('Track_id', 'track_id')
            df.columns = df.columns.str.replace('Track id', 'track_id')
            df.columns = df.columns.str.replace('track id', 'track_id')
            df.columns = df.columns.str.replace('TrackId', 'track_id')

            df.columns = df.columns.str.replace('Memo', 'memo')
            df.columns = df.columns.str.replace('s12', 'memo')
            df.columns = df.columns.str.replace('A12', 'memo')

            df.columns = df.columns.str.replace('Type', 'type')

            df.columns = df.columns.str.replace('Mp3_link', 'mp3_link')
            df.columns = df.columns.str.replace('MP3_link', 'mp3_link')
            df.columns = df.columns.str.replace('MP3link', 'mp3_link')
            df.columns = df.columns.str.replace('MP4_link', 'mp4_link')
            df.columns = df.columns.str.replace('MP4link', 'mp4_link')
            df.columns = df.columns.str.replace('Mp4_link', 'mp4_link')

            df.columns = df.columns.str.replace('url to add', 'url_to_add')
            df.columns = df.columns.str.replace('Url to add', 'url_to_add')
            df.columns = df.columns.str.replace('artist_url_to_add', 'url_to_add')

            df.columns = df.columns.str.replace('Remix_url', 'remix_url')
            df.columns = df.columns.str.replace('Remix_artist', 'remix_artist')
            df.columns = df.columns.str.replace('Live_url', 'live_url')
            df.columns = df.columns.str.replace('Live_venue', 'live_venue')
            df.columns = df.columns.str.replace('Live_year', 'live_year')

            df.columns = df.columns.str.replace('Uuid', 'uuid')
            df.columns = df.columns.str.replace('Artist_UUID', 'uuid')
            df.columns = df.columns.str.replace('objectid', 'uuid')

            df.columns = df.columns.str.replace('Content_to_add', 'content_to_add')
            df.columns = df.columns.str.replace('content tomadd', 'content_to_add')

            df = df[column_name]
            return df
        else:
            print(f"sheet_name: {sheet_name} not have")

    def media_file(self, sheet_info: object, page_priority: int):
        df = self.process_file(sheet_info=sheet_info)
        info = {"url": f"{self.url}",
                "gsheet_id": f"{self.gsheet_id}",
                "gsheet_name": f"{get_gsheet_name(gsheet_id=self.gsheet_id)}",
                "sheet_name": f"{sheet_info.get('sheet_name')}",
                "page_type": "top_single",
                "page_priority": page_priority
                }
        df['gsheet_info'] = df.apply(lambda x: json.dumps(info), axis=1)
        return df


def merge_file(sheet_name: str, urls: list, page_type: object = None):
    # Step 1: remove duplicate url
    url_reformats = list(
        set(map(lambda x: "https://docs.google.com/spreadsheets/d/" + get_gsheet_id_from_url(x), urls)))
    priority = page_type.priority

    df = pd.DataFrame()
    for url in url_reformats:
        try:
            page = Page(url=url)
            sheet_info = getattr(page.sheet_name_type, sheet_name)
            df_ = page.media_file(sheet_info=sheet_info, page_priority=priority)
            df = df.append(df_, ignore_index=True)
        except AttributeError:
            print(colored(f"AttributeError: {page.page_name} object has no attribute {sheet_name}\nurl: {url}",
                          'yellow'))
    return df


if __name__ == "__main__":
    start_time = time.time()
    pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)
    urls = [
        "https://docs.google.com/spreadsheets/d/1Ui62USL6FiN0Z6q88_jArnV18NeuaarakFfaQ1imm_c/edit#gid=1400083434",
        "https://docs.google.com/spreadsheets/d/1tRfdBnlDZ2MgBBmKr333Rz4qZpHWkgssym9S9WoybrE/edit#gid=1055124011"
    ]
    sheet_name = SheetNames.ARTIST_IMAGE
    page_type = PageType.TopSingle
    k = merge_file(sheet_name=sheet_name, urls=urls, page_type=page_type)
    print(k)



    print("\n --- total time to process %s seconds ---" % (time.time() - start_time))
