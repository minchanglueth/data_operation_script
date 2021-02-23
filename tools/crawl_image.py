from google_spreadsheet_api.function import get_df_from_speadsheet
import time
import pandas as pd
import numpy as np
from core import query_path


def craw_image(gsheet_id, sheet_name):
    pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)
    df = get_df_from_speadsheet(gsheet_id, sheet_name)
    artist_filter_df = df[
        (df.artist_uuid.notnull())  # filter df by conditions
        & (df.artist_uuid != '')
        & (df.artist_url_to_add.notnull())
        & (df.artist_url_to_add != '')
        & (df.checking == 'result: False')
        ][['artist_url_to_add', 'artist_uuid']].drop_duplicates(subset='artist_uuid',
                                                                keep='first')  # remove duplicate df by column (reset_index before drop_duplicate: because of drop_duplicate default reset index)
    artist_row_index = artist_filter_df.index

    # album_filter_df = df[
    #     (df.albums_uuid.notnull())  # filter df by conditions
    #     & (df.albums_uuid != '')
    #     & (df.album_url_to_add.notnull())
    #     & (df.album_url_to_add != '')
    # ][['album_url_to_add','albums_uuid']].drop_duplicates(subset='albums_uuid', keep='first')  # remove duplicate df by column (reset_index before drop_duplicate: because of drop_duplicate default reset index)
    # album_row_index = album_filter_df.index

    with open(query_path, "w") as f:
        for i in artist_row_index:
            x = artist_filter_df['artist_uuid'].loc[i]
            y = artist_filter_df['artist_url_to_add'].loc[i]
            query = f"insert into crawlingtasks(Id, ActionId,objectid ,TaskDetail, Priority) values (uuid4(), 'OA9CPKSUT6PBGI1ZHPLQUPQCGVYQ71S9','{x}',JSON_SET(IFNULL(crawlingtasks.TaskDetail, JSON_OBJECT()), '$.url','{y}','$.object_type',\"artist\",'$.when_exists',\"replace\",'$.PIC',\"Joy_xinh\"),999);"
            print(query)
        # for j in album_row_index:
        #     x = album_filter_df['albums_uuid'].loc[j]
        #     y = album_filter_df['album_url_to_add'].loc[j]
        #     query = f"insert into crawlingtasks(Id, ActionId,objectid ,TaskDetail, Priority) values (uuid4(), 'OA9CPKSUT6PBGI1ZHPLQUPQCGVYQ71S9','{x}',JSON_SET(IFNULL(crawlingtasks.TaskDetail, JSON_OBJECT()), '$.url','{y}','$.object_type',\"album\",'$.when_exists',\"replace\",'$.PIC',\"Joy_xinh\"),999);"
        #     print(query)
        f.write(query + "\n")


        # if albums_uuid == '':
        #     continue
        # else:
        #     query2 = query + f"insert into crawlingtasks(Id, ActionId,objectid ,TaskDetail, Priority) values (uuid4(), 'OA9CPKSUT6PBGI1ZHPLQUPQCGVYQ71S9','{albums_uuid}',JSON_SET(IFNULL(crawlingtasks.TaskDetail, JSON_OBJECT()), '$.url','{albums_url}','$.object_type','artist','$.when_exists','replace','$.PIC','Joy_xinh'),99) ;\n"
        #     print(query2)

        # f.write(query + "\n")


if __name__ == "__main__":
    start_time = time.time()
    # INPUT HERE:
    # Input_url 'https://docs.google.com/spreadsheets/d/1nm7DRUX0v1zODohS6J5LTDHP2Rew-OxSw8qN5FiplVk/edit#gid=274394075&fvid=750393421'
    gsheet_id = '1nm7DRUX0v1zODohS6J5LTDHP2Rew-OxSw8qN5FiplVk'
    sheet_name = 'wrong resize imgae'
    craw_image(gsheet_id, sheet_name)
