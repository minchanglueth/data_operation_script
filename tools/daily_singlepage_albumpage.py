import os
from core import BASE_DIR
from core.crud.sql.album import get_album_wiki
from core.crud.sql.track import get_track_wiki, get_track_lyric
from core.crud.sql.crawlingtask import get_crawl_image_status, get_artist_image_cant_crawl
from core.crud.get_df_from_query import get_df_from_query

from google_spreadsheet_api.function import get_list_of_sheet_title, update_value, get_df_from_speadsheet, \
    get_gsheet_name
from google_spreadsheet_api.create_new_sheet_and_update_data_from_df import creat_new_sheet_and_update_data_from_df

from support_function.automate_checking_crawler import automate_check_crawl_image_status

import time
import pandas as pd
import numpy as np
from core import query_path


# gsheet_id = input(f"\n Input gsheet_id: ").strip()
# sheet_name = input(f"\n Input sheet_name: ").strip()
# list_of_sheet_title = get_list_of_sheet_title(gsheet_id)


def upload_album_wiki():
    df = get_df_from_speadsheet(gsheet_id, sheet_name)
    album_uuid = df[(df.albumuuid != '') & (df.albumuuid != 'Album_UUID') & (df.albumuuid.notnull())][
        'albumuuid'].tolist()
    df_album_wiki = get_df_from_query(get_album_wiki(album_uuid))
    print("file to upload \n", df_album_wiki)

    time.sleep(3)
    print("\n Updating data")

    if 'wiki' in list_of_sheet_title:
        update_value(df_album_wiki.values.tolist(), 'wiki!A2', gsheet_id)
    else:
        creat_new_sheet_and_update_data_from_df(df_album_wiki, gsheet_id, 'wiki')


def upload_track_wiki():
    # Step 1: get data
    df = get_df_from_speadsheet(gsheet_id, sheet_name)
    track_id = tuple(df[(df.TrackId != '') & (df.TrackId != 'TrackId') & (df.TrackId.notnull())]['TrackId'].tolist())
    df_track_wiki = get_df_from_query(get_track_wiki(track_id))
    print("file to upload \n", df_track_wiki)

    time.sleep(3)
    print("\n Updating data")
    creat_new_sheet_and_update_data_from_df(df_track_wiki, gsheet_id, 'wiki')


def upload_track_lyrics():
    # Step 1: get data
    df = get_df_from_speadsheet(gsheet_id, sheet_name)
    trackid = tuple(df[(df.TrackId != '') & (df.TrackId != 'TrackId') & (df.TrackId.notnull())]['TrackId'].tolist())
    df_track_lyric = get_df_from_query(get_track_lyric(trackid))

    print("file to upload \n", df_track_lyric)

    time.sleep(3)
    print("\n Updating data")
    creat_new_sheet_and_update_data_from_df(df_track_lyric, gsheet_id, 'lyrics')


def automate_check_crawl_artist_image_status():  # need to optimize
    commit_message = input(f"\n Do you complete crawling_tasks insertion ?: True or False:")

    if commit_message == '1':
        count = 0
        while True and count < 300:
            df1 = get_df_from_query(get_crawl_image_status())
            result = df1[
                         (df1.status != 'complete')
                         & (df1.status != 'incomplete')
                         ].status.tolist() == []
            if result == 1:
                print('\n', 'Checking crawlingtask status \n', df1, '\n')
                break
            else:
                count += 1
                time.sleep(5)
                print(count, "-----", result)

    else:
        print("Please insert crawling_tasks")


def crawl_artist_image_singlepage():
    # Step 1: Select process sheet
    new_sheet_name = 'artist image cant upload'
    if new_sheet_name in list_of_sheet_title:
        filter_df = get_df_from_speadsheet(gsheet_id, new_sheet_name)
        print("List artist to crawl image \n ", filter_df, "\n")
    else:
        df = get_df_from_speadsheet(gsheet_id, sheet_name)
        filter_df = df[(df.s12 == 'missing')  # filter df by conditions
                       & (df.artist_url_to_add.notnull())
                       & (df.artist_url_to_add != '')
                       ].reset_index().drop_duplicates(subset=['Artist_UUID'],
                                                       keep='first')  # remove duplicate df by column (reset_index before drop_duplicate: because of drop_duplicate default reset index)
        print("List artist to crawl image \n ", filter_df[['ArtistName', 'Artist_UUID', 's12', 'artist_url_to_add']],
              "\n")

    # Step 2: get crawlingtask
    row_index = filter_df.index
    with open(query_path, "w") as f:
        for i in row_index:
            x = filter_df['Artist_UUID'].loc[i]
            y = filter_df['artist_url_to_add'].loc[i]
            query = f"insert into crawlingtasks(Id, ActionId,objectid ,TaskDetail, Priority) values (uuid4(), 'OA9CPKSUT6PBGI1ZHPLQUPQCGVYQ71S9','{x}',JSON_SET(IFNULL(crawlingtasks.TaskDetail, JSON_OBJECT()), '$.url','{y}','$.object_type',\"artist\",'$.when_exists',\"replace\",'$.PIC', '{gsheet_name}_{sheet_name}'),99);"
            f.write(query + "\n")

    # Step 3: automation check crawl_artist_image_status then export result:
    automate_check_crawl_image_status(gsheet_name=gsheet_name, sheet_name=sheet_name)
    #
    # # Step 4: upload artist image cant upload
    artist_uuid = filter_df['Artist_UUID'].tolist()
    df_artist_image_cant_upload = get_df_from_query(
        get_artist_image_cant_crawl(artist_uuid)).reset_index().drop_duplicates(subset=['Artist_UUID'],
                                                                                keep='first')  # remove duplicate df by column (reset_index before drop_duplicate: because of drop_duplicate default reset index)

    df_artist_image_cant_upload = df_artist_image_cant_upload.rename(columns={'uuid': 'Artist_UUID'})

    joy = df_artist_image_cant_upload[(df_artist_image_cant_upload.status == 'incomplete')].Artist_UUID.tolist() == []

    if joy == 1:
        raw_df_to_upload = {'status': ['Upload thành công 100% nhé các em ^ - ^']}
        df_to_upload = pd.DataFrame(data=raw_df_to_upload)
    else:
        df_to_upload = df_artist_image_cant_upload[(df_artist_image_cant_upload.status == 'incomplete')]

    creat_new_sheet_and_update_data_from_df(df_to_upload, gsheet_id, new_sheet_name)


def crawl_artist_image_albumpage():
    # Step 1: Select process sheet
    new_sheet_name = 'artist image cant upload'
    if new_sheet_name in list_of_sheet_title:
        filter_df = get_df_from_speadsheet(gsheet_id, new_sheet_name)
        print("List artist to crawl image \n ", filter_df, "\n")
    else:
        df = get_df_from_speadsheet(gsheet_id, sheet_name)
        filter_df = df[(df.A12 == 'missing')  # filter df by conditions
                       & (df.artist_url_to_add.notnull())
                       & (df.artist_url_to_add != '')
                       ].reset_index().drop_duplicates(subset=['uuid'],
                                                       keep='first')  # remove duplicate df by column (reset_index before drop_duplicate: because of drop_duplicate default reset index)
        print("List artist to crawl image \n ", filter_df[['ArtistName', 'Artist_UUID', 'A12', 'artist_url_to_add']],
              "\n")

    # Step 2: get crawlingtask
    row_index = filter_df.index
    with open(query_path, "w") as f:
        for i in row_index:
            x = filter_df['Artist_UUID'].loc[i]
            y = filter_df['artist_url_to_add'].loc[i]
            query = f"insert into crawlingtasks(Id, ActionId,objectid ,TaskDetail, Priority) values (uuid4(), 'OA9CPKSUT6PBGI1ZHPLQUPQCGVYQ71S9','{x}',JSON_SET(IFNULL(crawlingtasks.TaskDetail, JSON_OBJECT()), '$.url','{y}','$.object_type',\"artist\",'$.when_exists',\"replace\",'$.PIC', '{gsheet_name}_{sheet_name}'),99);"
            f.write(query + "\n")

    # Step 3: automation check crawl_artist_image_status then export result:
    automate_check_crawl_image_status(gsheet_name=gsheet_name, sheet_name=sheet_name)

    # Step 4: upload artist image cant upload
    artist_uuid = filter_df['Artist_UUID'].tolist()

    df_artist_image_cant_upload = get_df_from_query(
        get_artist_image_cant_crawl(artist_uuid)).reset_index().drop_duplicates(subset=['uuid'],
                                                                                keep='first')  # remove duplicate df by column (reset_index before drop_duplicate: because of drop_duplicate default reset index)
    df_artist_image_cant_upload = df_artist_image_cant_upload.rename(columns={'uuid': 'Artist_UUID'})
    joy = df_artist_image_cant_upload[(df_artist_image_cant_upload.status == 'incomplete')].Artist_UUID.tolist() == []

    if joy == 1:
        raw_df_to_upload = {'status': ['Upload thành công 100% nhé các em ^ - ^']}
        df_to_upload = pd.DataFrame(data=raw_df_to_upload)
    else:
        df_to_upload = df_artist_image_cant_upload[(df_artist_image_cant_upload.status == 'incomplete')]

    creat_new_sheet_and_update_data_from_df(df_to_upload, gsheet_id, new_sheet_name)


def check_wiki_before_update_data():  # both single page and album page
    df_wiki = get_df_from_speadsheet(gsheet_id, 'wiki')
    df_wiki_filter = df_wiki[(df_wiki.memo == 'added')]
    df_wiki_checking = df_wiki_filter[(df_wiki_filter.url_to_add == '')
                                      | (df_wiki_filter.content_to_add == '')
                                      | (df_wiki_filter.url_to_add.isnull())
                                      | (df_wiki_filter.content_to_add.isnull())].values.tolist()
    result = df_wiki_checking == []
    return result


def update_wiki_result_to_gsheet():  # both single page and album page
    sheet_name = 'wiki'
    df_wiki = get_df_from_speadsheet(gsheet_id, sheet_name)

    conditions = [  # create a list of condition => if true =>> update value tương ứng
        ((df_wiki['memo'] == 'not ok') | (df_wiki['memo'] == 'added')) & (df_wiki.content_to_add != 'none') & (
                df_wiki.url_to_add != 'none') & (df_wiki.content_to_add != '') & (df_wiki.url_to_add != ''),
        ((df_wiki['memo'] == 'not ok') | (df_wiki['memo'] == 'added')) & (
                (df_wiki.content_to_add == 'none') | (df_wiki.url_to_add == 'none') | (
                df_wiki.content_to_add == '') | (df_wiki.url_to_add == '')),
        True]
    values = ['wiki added', 'remove wiki', None]  # create a list of the values tương ứng với conditions ơ trên
    df_wiki['joy xinh'] = np.select(conditions,
                                    values)  # create a new column and use np.select to assign values to it using our lists as arguments
    column_title = ['Joy note']
    list_result = np.array(df_wiki['joy xinh']).reshape(-1,
                                                        1).tolist()  # Chuyển về list từ 1 chiều về 2 chiều sử dung Numpy
    list_result.insert(0, column_title)
    range_to_update = f"{sheet_name}!K1"

    update_value(list_result, range_to_update, gsheet_id)
    print("update data in gsheet completed")


def update_wiki_singlepage():
    sheet_name = 'wiki'
    # Step 1: checking data
    if check_wiki_before_update_data() == 0:
        print("missing wiki_url or wiki_content")

    # Step 2: Update data in database
    else:
        df_wiki = get_df_from_speadsheet(gsheet_id, sheet_name)
        df_wiki_filter = df_wiki[(df_wiki.memo == 'added') | (df_wiki.memo == 'not ok')]
        # print(df_wiki_filter)
        row_index = df_wiki_filter.index
        # print(row_index)
        with open(query_path, "w") as f:
            for i in row_index:
                id = df_wiki_filter.id.loc[i]
                url = df_wiki_filter.url_to_add.loc[i]
                content = df_wiki_filter.content_to_add.loc[i].replace('\'', '\\\'').replace("\"", "\\\"")

                joy_xinh = f"Update tracks set info =  Json_replace(Json_remove(info,'$.wiki'),'$.wiki_url','not ok') where id = '{id}';"
                query = ""
                if url != "" and content != "" and url != 'none' and content != 'none':
                    query = f"UPDATE tracks SET info = Json_set(if(info is null,JSON_OBJECT(),info), '$.wiki', JSON_OBJECT('brief', '{content}'), '$.wiki_url','{url}') WHERE id = '{id}';"
                else:
                    query = query
                f.write(joy_xinh + "\n" + query + "\n")
                # print(query)

        # Step 3: update gsheet
        update_wiki_result_to_gsheet()


def update_wiki_albumpage():
    sheet_name = 'wiki'
    # Step 1: checking data
    if check_wiki_before_update_data() == 0:
        print("missing wiki_url or wiki_content")

    # Step 2: Update data in database
    else:
        df_wiki = get_df_from_speadsheet(gsheet_id, sheet_name)
        df_wiki_filter = df_wiki[(df_wiki.memo == 'added') | (df_wiki.memo == 'not ok')]
        # print(df_wiki_filter)
        row_index = df_wiki_filter.index
        # print(row_index)
        with open(query_path, "w") as f:
            for i in row_index:
                uuid = df_wiki_filter.uuid.loc[i]
                url = df_wiki_filter.url_to_add.loc[i]
                content = df_wiki_filter.content_to_add.loc[i].replace('\'', '\\\'').replace("\"", "\\\"")

                joy_xinh = f"Update albums set info =  Json_replace(Json_remove(info,'$.wiki'),'$.wiki_url','not ok') where uuid = '{uuid}';"
                query = ""
                if url != "" and content != "" and url != 'none' and content != 'none':
                    query = f"UPDATE albums SET info = Json_set(if(info is null,JSON_OBJECT(),info), '$.wiki', JSON_OBJECT('brief', '{content}'), '$.wiki_url','{url}') WHERE uuid = '{uuid}';"
                else:
                    query = query
                f.write(joy_xinh + "\n" + query + "\n")
                # print(query)

        # Step 3: update gsheet
        update_wiki_result_to_gsheet()


if __name__ == "__main__":
    start_time = time.time()
    pd.set_option("display.max_rows", None, "display.max_columns", 30, 'display.width', 500)
    # INPUT HERE:
    # Input_url 'https://docs.google.com/spreadsheets/d/1S2HD6IL_QdgFNgdm3811JBY3pX9UE2Oa36C_tulpJBw/edit#gid=2066669079'

    gsheet_id = '1pdx9HvzUtKOLQw5u0et7sXeI3vMxCfinVk8aKAQ6aMw'  # Album page
    sheet_name = '15.02.2021'

    # Input_url 'https://docs.google.com/spreadsheets/d/1j_8tC9q9--6oXxmnNt1CkimBaNunaLl-p2UzrFrb2hk/edit#gid=737499827'
    # gsheet_id = '1Hfeo1IjJ9FuW6v7LQxrC1eZ7lI-BFVPg308JwKu0XY0'  # Single page
    # sheet_name = '11.01.2021'

    list_of_sheet_title = get_list_of_sheet_title(gsheet_id)
    gsheet_name = get_gsheet_name(gsheet_id=gsheet_id)

    # Start tool:
    # upload_album_wiki()
    # upload_track_wiki()
    # upload_track_lyrics()

    # crawl_artist_image_singlepage()
    crawl_artist_image_albumpage()

    # update_wiki_singlepage()
    # update_wiki_albumpage()

    print("\n --- total time to process %s seconds ---" % (time.time() - start_time))
