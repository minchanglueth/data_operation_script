from google_spreadsheet_api.function import get_df_from_speadsheet, get_list_of_sheet_title, update_value, \
    creat_new_sheet_and_update_data_from_df, get_gsheet_name
from core.models.crawlingtask_action_master import V4CrawlingTaskActionMaster
from core.models.data_source_format_master import DataSourceFormatMaster
from core.crud.sql.datasource import get_datasourceids_from_youtube_url_and_trackid, related_datasourceid
from core.crud.sql import artist, album, datasource
from core.crud.get_df_from_query import get_df_from_query
from core.crud.sql.query_supporter import get_crawlingtask_youtube_info, get_crawlingtask_info, \
    get_crawlingtask_status
import pandas as pd
import numpy as np
import time
from datetime import datetime
from core import query_path
from itertools import chain
from colorama import Fore, Style
import json
from Data_lake_process.crawlingtask import crawl_youtube, crawl_image
from Data_lake_process.class_definition import WhenExist, PageType, SheetNames, merge_file, Page, DataReports, \
    get_key_value_from_gsheet_info, add_key_value_from_gsheet_info, get_gsheet_id_from_url

import csv


class mp3_type:
    C = 'c'
    D = 'd'
    Z = 'z'


def youtube_check_box(page_name: str, df: object, sheet_name: str):
    df['len'] = df['url_to_add'].apply(lambda x: len(x))

    if page_name == "TopSingle" and sheet_name == SheetNames.MP3_SHEET_NAME:
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
    elif page_name == "TopSingle" and sheet_name == SheetNames.MP4_SHEET_NAME:
        youtube_check_box = df[~
        ((
                 (df['track_id'] != '')
                 & (df['memo'] == 'added')
                 & (df['len'] == 43)
                 & (df['checking_mp4'] == 'TRUE')
                 # & (df['already_existed'] == 'null')
         ) |
         (
                 (df['track_id'] != '')
                 & (df['memo'] == '')
                 & (df['url_to_add'] == '')
                 & ~((df['checking_mp4'] == 'TRUE')
                     # & (df['already_existed'] == 'null')
                     )
         ) |
         (
                 (df['track_id'] != '')
                 & (df['memo'] == 'not found')
                 & (df['len'] == 0)
                 & (df['checking_mp4'] == 'TRUE')
                 # & (df['already_existed'] == 'null')
         )
         )
        ]

    if youtube_check_box.empty:
        print(Fore.LIGHTYELLOW_EX + f"Pass check box" + Style.RESET_ALL)
        return True
    else:
        print(youtube_check_box)
        return False


def automate_check_status(gsheet_name: str, sheet_name: str, actionid: str):
    count = 0
    while True and count < 300:
        df1 = get_df_from_query(
            get_crawlingtask_status(gsheet_name=gsheet_name, sheet_name=sheet_name, actionid=actionid))
        result = df1[
                     (df1.status != 'complete')
                     & (df1.status != 'incomplete')
                     ].status.tolist() == []
        if result == 1:
            # print('\n', 'Checking crawlingtask status \n', df1, '\n')
            print(
                Fore.LIGHTYELLOW_EX + f"File: {gsheet_name}, sheet_name: {sheet_name} has been crawled complete already" + Style.RESET_ALL)
            break
        else:
            count += 1
            time.sleep(5)
            print(count, "-----", result)


def update_data_reports(gsheet_info: object, status: str = None, count_complete: int = 0, count_incomlete: int = 0,
                        notice: str = None):
    gsheet_name = get_key_value_from_gsheet_info(gsheet_info=gsheet_info, key='gsheet_name')
    sheet_name = get_key_value_from_gsheet_info(gsheet_info=gsheet_info, key='sheet_name')
    gsheet_id = get_key_value_from_gsheet_info(gsheet_info=gsheet_info, key='gsheet_id')
    gsheet_url = f"https://docs.google.com/spreadsheets/d/{gsheet_id}"
    # https://docs.google.com/spreadsheets/d/1MHDksbs-RKXhZZ-LRgRhVy_ldAxK8lSzyoJK4sA_Uyo
    print(f"updating data_reports gsheet_name: {gsheet_name}, type: {sheet_name}")
    reports_df = get_df_from_speadsheet(gsheet_id="1MHDksbs-RKXhZZ-LRgRhVy_ldAxK8lSzyoJK4sA_Uyo", sheet_name="demo")
    row_index = reports_df.index
    for i in row_index:
        reports_gsheet_name = reports_df['gsheet_name'].loc[i]
        reports_sheet_name = reports_df['type'].loc[i]
        if gsheet_name == reports_gsheet_name and sheet_name == reports_sheet_name:
            range_to_update = f"demo!A{i + 2}"
            break
        else:
            range_to_update = f"demo!A{row_index.stop + 2}"
    list_result = [
        [gsheet_name, gsheet_url, sheet_name, f"{datetime.now()}", status, count_complete, count_incomlete, notice]]
    update_value(list_result=list_result, range_to_update=range_to_update,
                 gsheet_id="1MHDksbs-RKXhZZ-LRgRhVy_ldAxK8lSzyoJK4sA_Uyo")


def checking_accuracy(df: object, actionid: str):
    df['check'] = ''
    df['status'] = ''
    df['crawlingtask_id'] = ''
    row_index = df.index
    for i in row_index:
        if actionid == V4CrawlingTaskActionMaster.ARTIST_ALBUM_IMAGE:
            objectid = df['uuid'].loc[i]
        elif actionid == V4CrawlingTaskActionMaster.DOWNLOAD_VIDEO_YOUTUBE:
            objectid = df['track_id'].loc[i]
        url = df.url_to_add.loc[i]
        gsheet_info = df.gsheet_info.loc[i]
        gsheet_name = get_key_value_from_gsheet_info(gsheet_info=gsheet_info, key='gsheet_name')
        sheet_name = get_key_value_from_gsheet_info(gsheet_info=gsheet_info, key='sheet_name')
        PIC_taskdetail = f"{gsheet_name}_{sheet_name}"
        # db_crawlingtask = get_crawlingtask_info(objectid=objectid, PIC=PIC_taskdetail, actionid=actionid)
        db_crawlingtask = get_crawlingtask_info(objectid=objectid, PIC="top100albums_0305_2021", actionid=actionid)

        if db_crawlingtask:
            status = db_crawlingtask.status
            crawlingtask_id = db_crawlingtask.id
            if url in db_crawlingtask.url:
                check_accuracy = True
            else:
                check_accuracy = f"crawlingtask_id: {db_crawlingtask.id}: uuid and url not match"
                print(check_accuracy)
        else:
            check_accuracy = f"file: {PIC_taskdetail}, uuid: {objectid} is missing"
            print(check_accuracy)
            status = 'missing'
            crawlingtask_id = 'missing'
        df.loc[i, 'check'] = check_accuracy
        df.loc[i, 'status'] = status
        df.loc[i, 'crawlingtask_id'] = crawlingtask_id
    return df


def automate_checking_status(df: object, actionid: str):
    gsheet_infos = list(set(df.gsheet_info.tolist()))
    count = 0
    while True and count < 300:
        checking_accuracy_result = checking_accuracy(df=df, actionid=actionid)
        result = checking_accuracy_result[
                     (checking_accuracy_result['status'] != 'complete')
                     & (checking_accuracy_result['status'] != 'incomplete')
                     ].status.tolist() == []
        if result == 1:
            for gsheet_info in gsheet_infos:
                gsheet_name = get_key_value_from_gsheet_info(gsheet_info=gsheet_info, key='gsheet_name')
                sheet_name = get_key_value_from_gsheet_info(gsheet_info=gsheet_info, key='sheet_name')
                print(
                    Fore.LIGHTYELLOW_EX + f"File: {gsheet_name}, sheet_name: {sheet_name} has been crawled complete already" + Style.RESET_ALL)
            break
        else:
            count += 1
            time.sleep(2)
            print(count, "-----", result)


def upload_image_cant_crawl(checking_accuracy_result: object, sheet_name: str):
    gsheet_infos = list(set(checking_accuracy_result.gsheet_info.tolist()))
    df_incomplete = checking_accuracy_result[(checking_accuracy_result['status'] == 'incomplete')].reset_index().copy()

    df_incomplete['url'] = df_incomplete['gsheet_info'].apply(
        lambda x: get_key_value_from_gsheet_info(gsheet_info=x, key='url'))
    df_incomplete['url_to_add'] = ''
    if sheet_name == SheetNames.ARTIST_IMAGE:
        df_incomplete['name'] = df_incomplete['uuid'].apply(
            lambda x: artist.get_one_by_id(artist_uuid=x).name)
    else:
        df_incomplete['title'] = df_incomplete['uuid'].apply(lambda x: artist.get_one_by_id(artist_uuid=x).title)
        df_incomplete['artist'] = df_incomplete['uuid'].apply(lambda x: album.get_one_by_id(album_uuid=x).artist)

    df_incomplete = df_incomplete[
        ['uuid', 'name', 'status', 'crawlingtask_id', 'url', 'memo', 'url_to_add']]

    for gsheet_info in gsheet_infos:
        url = get_key_value_from_gsheet_info(gsheet_info=gsheet_info, key='url')
        df_incomplete_to_upload = df_incomplete[df_incomplete['url'] == url].reset_index()
        count_incomplete = df_incomplete_to_upload.index.stop
        joy = df_incomplete_to_upload['status'].tolist() == []

        if joy:
            raw_df_to_upload = {'status': ['Upload thành công 100% nhé các em ^ - ^']}
            df_to_upload = pd.DataFrame(data=raw_df_to_upload)
            # Step 3.1: upload image cant crawl: update reports
            # update_data_reports(gsheet_info=gsheet_info, status=DataReports.status_type_done,
            #                     count_complete=0, count_incomlete=count_incomplete)
        else:
            df_to_upload = df_incomplete_to_upload.drop(['url', 'index'], axis=1)

            # update_data_reports(gsheet_info=get_gsheet_id_from_url(url), status=DataReports.status_type_processing,
            #                     count_complete=0, count_incomlete=count_incomplete)
        new_sheet_name = f"{sheet_name_} cant upload"
        print(df_to_upload)
        creat_new_sheet_and_update_data_from_df(df_to_upload, get_gsheet_id_from_url(url), new_sheet_name)


def crawl_youtube_mp4(df: object):
    row_index = df.index
    with open(query_path, "a+") as f:
        for i in row_index:
            memo = df['memo'].loc[i]
            new_youtube_url = df['url_to_add'].loc[i]
            track_id = df['track_id'].loc[i]
            old_youtube_url = df['mp3_link'].loc[i]
            gsheet_name = get_key_value_from_gsheet_info(gsheet_info=df['gsheet_info'].loc[i], key='gsheet_name')
            sheet_name_ = get_key_value_from_gsheet_info(gsheet_info=df['gsheet_info'].loc[i], key='sheet_name')
            gsheet_id = get_key_value_from_gsheet_info(gsheet_info=df['gsheet_info'].loc[i], key='gsheet_id')
            priority = get_key_value_from_gsheet_info(gsheet_info=df['gsheet_info'].loc[i], key='page_priority')

            query = ""
            if memo == "not ok" and new_youtube_url == "none":
                datasourceids = get_datasourceids_from_youtube_url_and_trackid(youtube_url=old_youtube_url,
                                                                               trackid=track_id,
                                                                               formatid=DataSourceFormatMaster.FORMAT_ID_MP4_FULL).all()
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
                    query = query + f"-- not existed datasourceid searched by youtube_url: {old_youtube_url}, trackid:  {track_id}, format_id: {DataSourceFormatMaster.FORMAT_ID_MP4_FULL} in gssheet_name: {gsheet_name}, gsheet_id: {gsheet_id}, sheet_name: {sheet_name_} ;\n"

            elif memo == "not ok" and new_youtube_url != "none":
                query = query + crawl_youtube(track_id=track_id,
                                              youtube_url=new_youtube_url,
                                              format_id=DataSourceFormatMaster.FORMAT_ID_MP4_FULL,
                                              when_exist=WhenExist.REPLACE,
                                              priority=priority,
                                              pic=f"{gsheet_name}_{sheet_name_}"
                                              )
            elif memo == "added":
                query = query + crawl_youtube(track_id=track_id,
                                              youtube_url=new_youtube_url,
                                              format_id=DataSourceFormatMaster.FORMAT_ID_MP4_FULL,
                                              when_exist=WhenExist.SKIP,
                                              priority=priority,
                                              pic=f"{gsheet_name}_{sheet_name_}")
            f.write(query)


def crawl_youtube_mp3(df: object):
    print(df.head(10))
    row_index = df.index
    with open(query_path, "a+") as f:
        for i in row_index:
            memo = df['memo'].loc[i]
            new_youtube_url = df['url_to_add'].loc[i]
            track_id = df['track_id'].loc[i]
            old_youtube_url = df['mp3_link'].loc[i]
            gsheet_name = get_key_value_from_gsheet_info(gsheet_info=df['gsheet_info'].loc[i], key='gsheet_name')
            sheet_name_ = get_key_value_from_gsheet_info(gsheet_info=df['gsheet_info'].loc[i], key='sheet_name')
            gsheet_id = get_key_value_from_gsheet_info(gsheet_info=df['gsheet_info'].loc[i], key='gsheet_id')
            priority = get_key_value_from_gsheet_info(gsheet_info=df['gsheet_info'].loc[i], key='page_priority')
            type = df['type'].loc[i]
            query = ""
            if memo == "not ok" and new_youtube_url == "none":
                datasourceids = get_datasourceids_from_youtube_url_and_trackid(youtube_url=old_youtube_url,
                                                                               trackid=track_id,
                                                                               formatid=DataSourceFormatMaster.FORMAT_ID_MP3_FULL).all()
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
                    query = query + f"-- not existed datasourceid searched by youtube_url: {old_youtube_url}, trackid:  {track_id}, format_id: {DataSourceFormatMaster.FORMAT_ID_MP3_FULL} in gssheet_name: {gsheet_name}, gsheet_id: {gsheet_id}, sheet_name: {sheet_name_} ;\n"

            elif memo == "not ok" and new_youtube_url != "none":
                query = query + crawl_youtube(track_id=track_id,
                                              youtube_url=new_youtube_url,
                                              format_id=DataSourceFormatMaster.FORMAT_ID_MP3_FULL,
                                              when_exist=WhenExist.REPLACE,
                                              priority=priority,
                                              pic=f"{gsheet_name}_{sheet_name_}"
                                              )
                if type in (mp3_type.C, mp3_type.Z):
                    query = query + crawl_youtube(track_id=track_id,
                                                  youtube_url=new_youtube_url,
                                                  format_id=DataSourceFormatMaster.FORMAT_ID_MP4_STATIC,
                                                  when_exist=WhenExist.SKIP,
                                                  priority=priority,
                                                  pic=f"{gsheet_name}_{sheet_name_}"
                                                  )
                elif type == mp3_type.D:
                    query = query + crawl_youtube(track_id=track_id,
                                                  youtube_url=new_youtube_url,
                                                  format_id=DataSourceFormatMaster.FORMAT_ID_MP4_LYRIC,
                                                  when_exist=WhenExist.SKIP,
                                                  priority=priority,
                                                  pic=f"{gsheet_name}_{sheet_name_}"
                                                  )

            elif memo == "added":
                query = query + crawl_youtube(track_id=track_id,
                                              youtube_url=new_youtube_url,
                                              format_id=DataSourceFormatMaster.FORMAT_ID_MP3_FULL,
                                              when_exist=WhenExist.SKIP,
                                              priority=priority,
                                              pic=f"{gsheet_name}_{sheet_name_}")
                if type in (mp3_type.C, mp3_type.Z):
                    query = query + crawl_youtube(track_id=track_id,
                                                  youtube_url=new_youtube_url,
                                                  format_id=DataSourceFormatMaster.FORMAT_ID_MP4_STATIC,
                                                  when_exist=WhenExist.SKIP,
                                                  priority=priority,
                                                  pic=f"{gsheet_name}_{sheet_name_}"
                                                  )
                elif type == mp3_type.D:
                    query = query + crawl_youtube(track_id=track_id,
                                                  youtube_url=new_youtube_url,
                                                  format_id=DataSourceFormatMaster.FORMAT_ID_MP4_LYRIC,
                                                  when_exist=WhenExist.SKIP,
                                                  priority=priority,
                                                  pic=f"{gsheet_name}_{sheet_name_}"
                                                  )
            f.write(query)


def query_pandas_to_csv(df: object, column: str):
    row_index = df.index
    with open(query_path, "w") as f:
        for i in row_index:
            line = df[column].loc[i]
            f.write(line)
    f.close()


class ImageWorking:
    def __init__(self, sheet_name: str, urls: list, page_type: object):
        original_file_ = merge_file(sheet_name=sheet_name, urls=urls, page_type=page_type)
        if original_file_.empty:
            print("original_file is empty")
            pass
        else:
            self.original_file = original_file_
            self.sheet_name = sheet_name
            self.page_type = page_type

    def image_filter(self):
        df = self.original_file
        filter_df = df[((df['memo'] == 'missing') | (df['memo'] == 'added'))  # filter df by conditions
                       & (df['url_to_add'].notnull())
                       & (df['url_to_add'] != '')
                       ].drop_duplicates(subset=['uuid', 'url_to_add', 'gsheet_info'], keep='first').reset_index()

        if self.sheet_name == SheetNames.ARTIST_IMAGE:
            object_type_ = {"object_type": "artist"}
            filter_df['gsheet_info'] = filter_df.apply(
                lambda x: add_key_value_from_gsheet_info(gsheet_info=x['gsheet_info'], key_value=object_type_), axis=1)

        elif self.sheet_name == SheetNames.ALBUM_IMAGE:
            object_type_ = {"object_type": "album"}
            filter_df['gsheet_info'] = filter_df['gsheet_info'].apply(
                lambda x: add_key_value_from_gsheet_info(gsheet_info=x, key_value=object_type_))
        else:
            pass
        return filter_df

    def crawl_image_datalake(self, when_exists: str = WhenExist.REPLACE):
        df = self.image_filter()
        if df.empty:
            print(Fore.LIGHTYELLOW_EX + f"Image file is empty" + Style.RESET_ALL)
        else:
            df['query'] = df.apply(lambda x: crawl_image(
                object_type=get_key_value_from_gsheet_info(gsheet_info=x['gsheet_info'], key='object_type'),
                url=x['url_to_add'], objectid=x['uuid'],
                when_exists=when_exists,
                pic=f"{get_key_value_from_gsheet_info(gsheet_info=x['gsheet_info'], key='gsheet_name')}_{get_key_value_from_gsheet_info(gsheet_info=x['gsheet_info'], key='sheet_name')}",
                priority=get_key_value_from_gsheet_info(gsheet_info=df['gsheet_info'], key='page_priority')),
                                   axis=1)
            query_pandas_to_csv(df=df, column='query')

    def checking_image_crawler_status(self):
        print("checking accuracy")
        df = self.image_filter().copy()
        gsheet_infos = list(set(df.gsheet_info.tolist()))
        # step 1.1: checking accuracy
        checking_accuracy_result = checking_accuracy(df=df, actionid=V4CrawlingTaskActionMaster.ARTIST_ALBUM_IMAGE)
        accuracy_checking = list(set(checking_accuracy_result['check'].tolist()))

        if accuracy_checking != [True]:
            print(checking_accuracy_result[['uuid', 'check', 'status', 'crawlingtask_id']])
            # Step 1.2: update data_reports if checking accuracy fail
            for gsheet_info in gsheet_infos:
                update_data_reports(gsheet_info=gsheet_info, status=DataReports.status_type_processing,
                                    notice="check accuracy fail")
        # Step 2: auto checking status
        else:
            print("checking accuracy correctly, now checking status")
            automate_checking_status(df=df, actionid=V4CrawlingTaskActionMaster.ARTIST_ALBUM_IMAGE)
            # Step 3: upload image cant crawl
            upload_image_cant_crawl(checking_accuracy_result=checking_accuracy_result, sheet_name=self.sheet_name)


class YoutubeWorking:
    def __init__(self, sheet_name: str, urls: list, page_type: object):
        original_file_ = merge_file(sheet_name=sheet_name, urls=urls, page_type=page_type)
        if original_file_.empty:
            print("original_file is empty")
            pass
        else:
            self.original_file = original_file_
            self.sheet_name = sheet_name
            self.page_type = page_type

    def check_box(self):
        df = self.original_file
        youtube_check_box(page_name=getattr(self.page_type, "name"), df=df, sheet_name=self.sheet_name)
        return youtube_check_box

    def youtube_filter(self):
        if self.check_box():
            if self.sheet_name == SheetNames.MP3_SHEET_NAME:
                df = self.original_file
                filter_df = df[((df['memo'] == 'not ok') | (df['memo'] == 'added'))  # filter df by conditions
                               & (df['url_to_add'].notnull())
                               & (df['url_to_add'] != '')
                               ].drop_duplicates(subset=['track_id', 'url_to_add', 'type', 'gsheet_info'],
                                                 keep='first').reset_index()
                return filter_df

    def crawl_mp3_mp4_youtube_datalake(self):
        df = self.youtube_filter()
        if self.sheet_name == SheetNames.MP3_SHEET_NAME:
            crawl_youtube_mp3(df=df)
        elif self.sheet_name == SheetNames.MP4_SHEET_NAME:
            crawl_youtube_mp4(df=df)
        else:
            pass

    def checking_youtube_crawler_status(self):
        print("checking accuracy")
        df = self.youtube_filter().copy()
        gsheet_infos = list(set(df.gsheet_info.tolist()))
        # step 1.1: checking accuracy
        checking_accuracy_result = checking_accuracy(df=df, actionid=V4CrawlingTaskActionMaster.DOWNLOAD_VIDEO_YOUTUBE)
        accuracy_checking = list(set(checking_accuracy_result['check'].tolist()))
        if accuracy_checking != [True]:
            print(checking_accuracy_result[['uuid', 'check', 'status', 'crawlingtask_id']])
            # Step 1.2: update data_reports if checking accuracy fail
            for gsheet_info in gsheet_infos:
                update_data_reports(gsheet_info=gsheet_info, status=DataReports.status_type_processing,
                                    notice="check accuracy fail")
        # Step 2: auto checking status
        else:
            print("checking accuracy correctly, now checking status")
            automate_checking_status(df=df, actionid=V4CrawlingTaskActionMaster.DOWNLOAD_VIDEO_YOUTUBE)
    #     # Step 3: upload image cant crawl
    #         upload_image_cant_crawl(checking_accuracy_result=checking_accuracy_result, sheet_name=self.sheet_name)


def process_wiki(urls: list, sheet_info: dict):
    wiki_df = pd.DataFrame()
    for url in urls:
        gsheet_id = get_gsheet_id_from_url(url=url)
        list_of_sheet_title = get_list_of_sheet_title(gsheet_id)
        sheet_name = sheet_info['sheet_name']
        if sheet_name in list_of_sheet_title:
            original_df = get_df_from_speadsheet(gsheet_id, sheet_name)
        else:
            continue
        #     # Refactor column name before put into datalake
        original_df.columns = original_df.columns.str.replace('Artist_UUID', 'uuid')
        original_df.columns = original_df.columns.str.replace('Album_uuid', 'uuid')
        original_df.columns = original_df.columns.str.replace('Artist_uuid', 'uuid')
        original_df.columns = original_df.columns.str.replace('id', 'uuid')
        original_df.columns = original_df.columns.str.replace('uuuuid', 'uuid')
        original_df.columns = original_df.columns.str.replace('memo', 'Memo')
        original_df.columns = original_df.columns.str.replace('content to add', 'content_to_add')
        original_df.columns = original_df.columns.str.replace('Content_to_add', 'content_to_add')
        original_df.columns = original_df.columns.str.replace('Url_to_add', 'url_to_add')

        wiki = original_df[sheet_info['column_name']]
        filter_df = wiki[
            ((wiki.Memo == 'added') | (wiki.Memo == 'not ok'))  # filter df by conditions
        ].reset_index().drop_duplicates(subset=['uuid'],
                                        keep='first')  # remove duplicate df by column (reset_index before drop_duplicate: because of drop_duplicate default reset index)
        info = {"url": f"{url}", "gsheet_id": f"{gsheet_id}",
                "gsheet_name": f"{get_gsheet_name(gsheet_id=gsheet_id)}",
                "sheet_name": f"{sheet_name}"}
        filter_df['gsheet_info'] = f"{info}"
        wiki_df = wiki_df.append(filter_df, ignore_index=True)
    return wiki_df


def update_wiki_result_to_gsheet(sheet_name: str, gsheet_id: str):  # both single page and album page
    df_wiki = get_df_from_speadsheet(gsheet_id, sheet_name)
    df_wiki.columns = df_wiki.columns.str.replace('Artist_UUID', 'uuid')
    df_wiki.columns = df_wiki.columns.str.replace('Album_uuid', 'uuid')
    df_wiki.columns = df_wiki.columns.str.replace('Artist_uuid', 'uuid')
    df_wiki.columns = df_wiki.columns.str.replace('id', 'uuid')
    df_wiki.columns = df_wiki.columns.str.replace('uuuuid', 'uuid')
    df_wiki.columns = df_wiki.columns.str.replace('memo', 'Memo')
    df_wiki.columns = df_wiki.columns.str.replace('content to add', 'content_to_add')
    df_wiki.columns = df_wiki.columns.str.replace('Content_to_add', 'content_to_add')
    df_wiki.columns = df_wiki.columns.str.replace('Url_to_add', 'url_to_add')

    conditions = [  # create a list of condition => if true =>> update value tương ứng
        ((df_wiki['Memo'] == 'not ok') | (df_wiki['Memo'] == 'added')) & (df_wiki['content_to_add'] != 'none') & (
                df_wiki.url_to_add != 'none') & (df_wiki['content_to_add'] != '') & (df_wiki.url_to_add != ''),
        ((df_wiki['Memo'] == 'not ok') | (df_wiki['Memo'] == 'added')) & (
                (df_wiki['content_to_add'] == 'none') | (df_wiki.url_to_add == 'none') | (
                df_wiki['content_to_add'] == '') | (df_wiki.url_to_add == '')),
        True]
    values = ['wiki added', 'remove wiki', None]  # create a list of the values tương ứng với conditions ơ trên
    df_wiki['joy xinh'] = np.select(conditions,
                                    values)  # create a new column and use np.select to assign values to it using our lists as arguments
    column_title = ['Joy note']
    list_result = np.array(df_wiki['joy xinh']).reshape(-1,
                                                        1).tolist()  # Chuyển về list từ 1 chiều về 2 chiều sử dung Numpy
    list_result.insert(0, column_title)
    range_to_update = f"{sheet_name}!J1"

    update_value(list_result, range_to_update, gsheet_id)


def update_wiki(df: object, sheet_info: object):
    '''
    ARTIST_WIKI = {"sheet_name": "Artist_wiki", "column_name": ["Artist_uuid", "Memo", "url_to_add", "content to add"], "table_name": "artists"}
    ALBUM_WIKI = {"sheet_name": "Album_wiki", "column_name": ["Album_uuid", "Memo", "url_to_add", "content to add"], "table_name": "albums"}
    :param sheet_info:
    :return:
    '''

    row_index = df.index
    # column_name = sheet_info['column_name']
    table_name = sheet_info['table_name']
    # sheet_name = sheet_info['sheet_name']
    with open(query_path, "w") as f:
        for i in row_index:
            uuid = df.uuid.loc[i]
            url = df.url_to_add.loc[i]
            content = df.content_to_add.loc[i].replace('\'', '\\\'').replace("\"", "\\\"")
            if table_name == "tracks":
                column_name = "id"
            else:
                column_name = "uuid"
            joy_xinh = f"Update {table_name} set info =  Json_replace(Json_remove(info,'$.wiki'),'$.wiki_url','not ok') where {column_name} = '{uuid}';"
            query = ""
            if url != "" and content != "" and url != 'none' and content != 'none':
                query = f"UPDATE {table_name} SET info = Json_set(if(info is null,JSON_OBJECT(),info), '$.wiki', JSON_OBJECT('brief', '{content}'), '$.wiki_url','{url}') WHERE {column_name} = '{uuid}';"
            else:
                query = query
            f.write(joy_xinh + "\n" + query + "\n")
            print(joy_xinh + "\n" + query + "\n")

        # Step 3: update gsheet

        gsheet_info_all = list(set(df.gsheet_info.tolist()))
        for gsheet_info in gsheet_info_all:
            gsheet_info = gsheet_info.replace("'", "\"")
            sheet_name = json.loads(gsheet_info)['sheet_name']
            gsheet_id = json.loads(gsheet_info)['gsheet_id']
            update_wiki_result_to_gsheet(sheet_name=sheet_name, gsheet_id=gsheet_id)


def process_S_11(urls: list, sheet_info: dict):
    '''
    S_11 = {"sheet_name": "S_11",
            "column_name": ["release_date", "album_title", "album_artist", "itune_album_url", "sportify_album_url"]}
    '''
    S_11_df = pd.DataFrame()
    for url in urls:
        gsheet_id = get_gsheet_id_from_url(url=url)
        sheet_name = sheet_info['sheet_name']
        original_df = get_df_from_speadsheet(gsheet_id, sheet_name)
        #     # Refactor column name before put into datalake
        original_df.columns = original_df.columns.str.replace('Release_date', 'release_date')
        original_df.columns = original_df.columns.str.replace('AlbumTitle', 'album_title')
        original_df.columns = original_df.columns.str.replace('AlbumArtist', 'album_artist')
        original_df.columns = original_df.columns.str.replace('Itunes_Album_URL', 'itune_album_url')
        original_df.columns = original_df.columns.str.replace('AlbumURL', 'sportify_album_url')
        filter_df = original_df[(original_df.itune_album_url != 'not found')].reset_index()
        info = {"url": f"{url}", "gsheet_id": f"{gsheet_id}",
                "gsheet_name": f"{get_gsheet_name(gsheet_id=gsheet_id)}",
                "sheet_name": f"{sheet_name}"}
        filter_df['gsheet_info'] = f"{info}"
        S_11_df = S_11_df.append(filter_df, ignore_index=True)
    return S_11_df


# def control_flow(sheet_name: str, urls: list, page_tye: object):
#     if sheet_name ==SheetNames.MP3_SHEET_NAME:
#         image_working = ImageWorking(sheet_name=sheet_name_, urls=urls, page_type=page_type_)

class ControlFlow:
    def __init__(self, sheet_name: str, urls: list, page_type: object):
        self.page_type = page_type
        self.urls = urls
        self.sheet_name = sheet_name

    def check_box(self):
        if self.sheet_name in (SheetNames.ARTIST_IMAGE, SheetNames.ALBUM_IMAGE):
            image_working = ImageWorking(sheet_name=self.sheet_name, urls=self.urls, page_type=self.page_type)
            image_filter = image_working.check_box()
            return image_filter
        elif self.sheet_name in (SheetNames.MP3_SHEET_NAME, SheetNames.MP4_SHEET_NAME):
            youtube_working = YoutubeWorking(sheet_name=self.sheet_name, urls=self.urls, page_type=self.page_type)
            check_box = youtube_working.check_box()

    def observe(self):
        if self.sheet_name in (SheetNames.ARTIST_IMAGE, SheetNames.ALBUM_IMAGE):
            image_working = ImageWorking(sheet_name=self.sheet_name, urls=self.urls, page_type=self.page_type)
            image_filter = image_working.image_filter()
            return image_filter
        elif self.sheet_name in (SheetNames.MP3_SHEET_NAME, SheetNames.MP4_SHEET_NAME):
            youtube_working = YoutubeWorking(sheet_name=self.sheet_name, urls=self.urls, page_type=self.page_type)
            youtube_filter = youtube_working.youtube_filter()
            return youtube_filter

    def crawl(self):
        if self.sheet_name in (SheetNames.ARTIST_IMAGE, SheetNames.ALBUM_IMAGE):
            image_working = ImageWorking(sheet_name=self.sheet_name, urls=self.urls, page_type=self.page_type)
            image_working.crawl_image_datalake()

        elif self.sheet_name in (SheetNames.MP3_SHEET_NAME, SheetNames.MP4_SHEET_NAME):
            youtube_working = YoutubeWorking(sheet_name=self.sheet_name, urls=self.urls, page_type=self.page_type)
            youtube_working.crawl_mp3_mp4_youtube_datalake()

    def checking(self):
        if self.sheet_name in (SheetNames.ARTIST_IMAGE, SheetNames.ALBUM_IMAGE):
            image_working = ImageWorking(sheet_name=self.sheet_name, urls=self.urls, page_type=self.page_type)
            image_working.checking_image_crawler_status()

        elif self.sheet_name in (SheetNames.MP3_SHEET_NAME, SheetNames.MP4_SHEET_NAME):
            youtube_working = YoutubeWorking(sheet_name=self.sheet_name, urls=self.urls, page_type=self.page_type)
            youtube_working.checking_youtube_crawler_status()


if __name__ == "__main__":
    start_time = time.time()

    pd.set_option("display.max_rows", None, "display.max_columns", 30, 'display.width', 500)
    with open(query_path, "w") as f:
        f.truncate()
    urls = [
        "https://docs.google.com/spreadsheets/d/17oTEIcl8BFiUcD75Qq0JtI7MO1VqMax3Re7nQQ_SIgI/edit#gid=0",
        # "https://docs.google.com/spreadsheets/d/1ciYEVsgH-kmuutirH07n9rOG2CZPxr-M6tT0H3mTfEY/edit#gid=1541562889"
    ]

    sheet_name_ = SheetNames.MP4_SHEET_NAME
    page_type_ = PageType.TopSingle

    # joy_xinh = YoutubeWorking(sheet_name=sheet_name_, urls=urls, page_type=page_type_)
    # joy_xinh.check_box()
    # k = joy_xinh.original_file

    # print(k.head(10))

    control_flow = ControlFlow(sheet_name=sheet_name_, urls=urls, page_type=page_type_)
    # check_box:
    # joy_xinh = control_flow.check_box()

    # observe:
    k = control_flow.observe()
    print(k)

    # crawl:
    # control_flow.crawl()

    # # checking
    # control_flow.checking()

    print("\n --- total time to process %s seconds ---" % (time.time() - start_time))
