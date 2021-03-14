from google_spreadsheet_api.function import get_df_from_speadsheet, get_list_of_sheet_title, update_value, \
    creat_new_sheet_and_update_data_from_df, get_gsheet_name

from core.crud.sql.datasource import get_datasourceids_from_youtube_url_and_trackid, related_datasourceid, \
    get_youtube_info_from_trackid
from core.crud.get_df_from_query import get_df_from_query

from tools.crawlingtask import crawl_youtube, WhenExist
from core.models.data_source_format_master import DataSourceFormatMaster
from core.models.crawlingtask_action_master import V4CrawlingTaskActionMaster

import pandas as pd
import time
from core import query_path
from itertools import chain
from tools.crawlingtask import sheet_type


def get_sheet_info_from_url(url: str):
    url_list = url.split("/")
    gsheet_id = url_list[5]
    gsheet_name = get_gsheet_name(gsheet_id=gsheet_id)
    list_of_sheet_title = get_list_of_sheet_title(gsheet_id=gsheet_id)
    return {"gsheet_id": gsheet_id, "gsheet_name": gsheet_name, "list_of_sheet_title": list_of_sheet_title}


def crawl_mp3_mp4(df: object, sheet_info: dict):
    row_index = df.index
    old_youtube_url_column_name = sheet_info['column_name'][2]
    datasource_format_id = sheet_info['fomatid']
    with open(query_path, "a+") as f:
        for i in row_index:
            memo = df.Memo.loc[i]
            new_youtube_url = df.url_to_add.loc[i]
            track_id = df.track_id.loc[i]
            old_youtube_url = df[old_youtube_url_column_name].loc[i]
            query = ""
            sheet_info_log = df.gsheet_info.loc[i]
            sheet_info_log = sheet_info_log.replace("'", "\"")
            import json
            gsheet_name = json.loads(sheet_info_log)['gsheet_name']
            sheet_name = json.loads(sheet_info_log)['sheet_name']
            gsheet_id = json.loads(sheet_info_log)['gsheet_id']

            if memo == "not ok" and new_youtube_url == "none":
                datasourceids = get_datasourceids_from_youtube_url_and_trackid(youtube_url=old_youtube_url,
                                                                               trackid=track_id,
                                                                               formatid=datasource_format_id).all()
                datasourceids_flatten_list = tuple(set(list(chain.from_iterable(datasourceids))))  # flatten list

                if datasourceids_flatten_list:
                    for datasourceid in datasourceids_flatten_list:
                        related_id_datasource = related_datasourceid(datasourceid)
                        if related_id_datasource == [(None, None, None)]:
                            query = query + f"Update datasources set valid = -10 where id = {datasourceid};\n"
                        else:
                            query = query + f"UPDATE datasources SET trackid = '', FormatID = ''  where id = '{datasourceid}';\n"
                            query = query + f"UPDATE datasources SET updatedAt = NOW() WHERE trackid = '{track_id}';\n"

                else:
                    query = query + f"--not existed datasourceid searched by youtube_url: {old_youtube_url}, trackid:  {track_id}, format_id: {datasource_format_id} in gssheet_name: {gsheet_name}, gsheet_id: {gsheet_id}, sheet_name: {sheet_name} ;\n"

            elif memo == "not ok" and new_youtube_url != "none":
                query = query + crawl_youtube(track_id=track_id, youtube_url=new_youtube_url,
                                              format_id=sheet_info.get('fomatid'),
                                              when_exist=WhenExist.REPLACE, pic=f"{gsheet_name}_{sheet_name}")
            elif memo == "added":
                query = query + crawl_youtube(track_id=track_id, youtube_url=new_youtube_url,
                                              format_id=sheet_info.get('fomatid'),
                                              when_exist=WhenExist.SKIP, pic=f"{gsheet_name}_{sheet_name}")
            print(query)
            f.write(query)

def checking_crawlingtask_status(df: object, sheet_info: dict):
    '''
    :param sheet_info:
    MP3_SHEET_NAME = {"sheet_name": "MP_3", "fomatid": DataSourceFormatMaster.FORMAT_ID_MP3_FULL,
                      "column_name": ["track_id", "Memo", "Mp3_link", "url_to_add"]}
    MP4_SHEET_NAME = {"sheet_name": "MP_4", "fomatid": DataSourceFormatMaster.FORMAT_ID_MP4_FULL,
                      "column_name": ["track_id", "Memo", "MP4_link", "url_to_add"]}
    VERSION_SHEET_NAME = {"sheet_name": "Version_done", "fomatid": [DataSourceFormatMaster.FORMAT_ID_MP4_REMIX,
                                                                    DataSourceFormatMaster.FORMAT_ID_MP4_LIVE],
                          "column_name": ["track_id", "Remix_url", "Remix_artist", "Live_url", "Live_venue",
                                          "Live_year"]}

    ARTIST_IMAGE = {"sheet_name": "Artist_image", "column_name": ["Artist_uuid", "Memo", "url_to_add"],
                    "object_type": "artist"}
    ALBUM_IMAGE = {"sheet_name": "Album_image", "column_name": ["Album_uuid", "Memo", "url_to_add"],
                   "object_type": "album"}

    ARTIST_WIKI = {"sheet_name": "Artist_wiki", "column_name": ["Artist_uuid", "Memo", "url_to_add", "content to add"],
                   "table_name": "artists"}
    ALBUM_WIKI = {"sheet_name": "Album_wiki", "column_name": ["Album_uuid", "Memo", "url_to_add", "Content_to_add"],
                  "table_name": "albums"}
    :return:
    '''

    # Step 2: Check accuracy
    crawl_image_status_df = get_df_from_query()
    # merge_df = pd.merge(left=filter_df, right=crawl_image_status_df, left_on=[column_name[0], column_name[2]], right_on= [])

    # Step 2: automation check crawl_artist_image_status then export result:
    # automate_check_crawl_image_status(gsheet_name=gsheet_name, sheet_name=sheet_name)

    # Step 4: upload image cant upload

    # uuid = filter_df[column_name[0]].tolist()
    # if sheet_name == "Artist_image":
    #     df_image_cant_upload = get_df_from_query(
    #         get_artist_image_cant_crawl(uuid)).reset_index().drop_duplicates(subset=['uuid'],
    #                                                                          keep='first')  # remove duplicate df by column (reset_index before drop_duplicate: because of drop_duplicate default reset index)
    # elif sheet_name == "Album_image":
    #     df_image_cant_upload = get_df_from_query(
    #         get_album_image_cant_crawl(uuid)).reset_index().drop_duplicates(subset=['uuid'],
    #                                                                         keep='first')
    #
    # joy = df_image_cant_upload[(df_image_cant_upload.status == 'incomplete')].uuid.tolist() == []
    #
    # if joy == 1:
    #     raw_df_to_upload = {'status': ['Upload thành công 100% nhé các em ^ - ^']}
    #     df_to_upload = pd.DataFrame(data=raw_df_to_upload)
    # else:
    #     df_to_upload = df_image_cant_upload[(df_image_cant_upload.status == 'incomplete')]

    # new_sheet_name = f"{sheet_name} cant upload"
    # creat_new_sheet_and_update_data_from_df(df_to_upload, gsheet_id, new_sheet_name)


if __name__ == "__main__":
    start_time = time.time()
    pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)
    with open(query_path, "w") as f:
        f.truncate()

    urls = [
        "https://docs.google.com/spreadsheets/d/1L17AVvNANbHYyLFKXlYRBEol8nZ14pvO3_KSGlQf0WE/edit#gid=1241019472",
        "https://docs.google.com/spreadsheets/d/14tBa8mqCAXw50qcQfZsrTs70LeKIPEe9yISsXxYgQUk/edit",
        "https://docs.google.com/spreadsheets/d/1qIfm2xhAIRb6kN6P1MgHMTNhlBYpLTApoMSyNa2fxk0/edit#gid=704433363",
        # "https://docs.google.com/spreadsheets/d/1Mbe1_ANXUMps_LONbn8ntaARg5JqU7v1dMU8KUpnCwo/edit#gid=408034383",
        # "https://docs.google.com/spreadsheets/d/16vY2NdX8IVHbeg7cHW2gSKsVd8CJiSadN33QKTz0cII/edit#gid=1243013907"
    ]

    sheet_info = sheet_type.MP4_SHEET_NAME


    mp3_mp4_df = pd.DataFrame()
    for url in urls:
        sheet_name = sheet_info['sheet_name']
        url_list = url.split("/")
        gsheet_id = url_list[5]
        original_df = get_df_from_speadsheet(gsheet_id, sheet_name)
        youtube_url = original_df[sheet_info['column_name']]
        filter_df = youtube_url[
            (youtube_url['Memo'] == 'not ok') | (youtube_url['Memo'] == 'added')].reset_index().drop_duplicates(
            subset=['track_id'],
            keep='first')  # remove duplicate df by column (reset_index before drop_duplicate: because of drop_duplicate default reset index)
        # info = f"{url}, {gsheet_id}, {get_gsheet_name(gsheet_id=gsheet_id)}, {sheet_name}"
        # filter_df['gsheet_info'] = info

        info = {"url": f"{url}", "gsheet_id": f"{gsheet_id}",
                "gsheet_name": f"{get_gsheet_name(gsheet_id=gsheet_id)}",
                "sheet_name": f"{sheet_name}"}
        filter_df['gsheet_info'] = f"{info}"
        mp3_mp4_df = mp3_mp4_df.append(filter_df, ignore_index=True)
        # print(mp3_mp4_df)

    crawl_mp3_mp4(mp3_mp4_df, sheet_info=sheet_info)

