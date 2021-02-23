from google_spreadsheet_api.function import update_value
from google_spreadsheet_api.function import get_df_from_speadsheet
from core.crud.get_df_from_query import get_df_from_query
from core.crud.sql.user import get_user_uuid_from_user_name
from core.crud.sql.artist import get_uuid_and_count_from_artist_name
from core.crud.sql.genre import get_genre_uuid_from_genre_name
from core.crud.sql.crawlingtask import get_crawl_E5_06_status, get_datasourceId_from_crawlingtask
from core.crud.sql.external_identity import get_trackid_from_ituneid_and_tracknum
from youtube_dl_fuction.fuctions import get_raw_title_uploader_from_youtube_url

from support_function.text_similarity.text_similarity import get_token_set_ratio

from tools.get_uuid4 import get_uuid4
import time
import pandas as pd
from core import query_path
from numpy import random
import numpy as np

def intern_checking_process():
    df = get_df_from_speadsheet(gsheet_id=gsheet_id, sheet_name= "iTunes checking!B3:V10000").apply(lambda x: x.str.strip())
    df = df.head(30)
    row_index = df.index
    get_youtube_titles = []
    get_youtube_uploaders = []
    for i in row_index:
        youtube_url = df.youtube_url.loc[i]
        status = df['recheck_id'].loc[i]
        if status == "ok":
            get_youtube_info = get_raw_title_uploader_from_youtube_url(youtube_url)
            get_youtube_title = get_youtube_info['youtube_title']
            get_youtube_uploader = get_youtube_info['uploader']
        else:
            get_youtube_title = "not to check"
            get_youtube_uploader = "not to check"

        get_youtube_titles.append(get_youtube_title)
        get_youtube_uploaders.append(get_youtube_uploader)

        x = random.uniform(0.5, 3)
        time.sleep(x)


    se_youtube_title = pd.Series(get_youtube_titles)
    se_youtube_uploader = pd.Series(get_youtube_uploaders)
    df['get_youtube_title'] = se_youtube_title.values
    df['get_youtube_uploader'] = se_youtube_uploader.values
    print(df)



def loop_data_from_intern_process():
    '''
    :return:“one_to_one” or “1:1”: check if merge keys are unique in both left and right datasets.
    “one_to_many” or “1:m”: check if merge keys are unique in left dataset.
    “many_to_one” or “m:1”: check if merge keys are unique in right dataset.
    “many_to_many” or “m:m”: allowed, but does not result in checks.
    '''
    intern_checking_file = get_df_from_speadsheet(gsheet_id=gsheet_id, sheet_name= "iTunes checking!B3:V10000").applymap(
        str.lower).apply(lambda x: x.str.strip())
    intern_checking_file = intern_checking_file[['artist_of_the_collection','single_title', 'youtube_url', 'filename','recheck_id', 'track_title_no', 'itunes_id', 'album_region', 'itunes_album_link', 'version', 'artist_cover']]

    merge_column = ['artist_of_the_collection', 'single_title', 'youtube_url', 'filename']
    merge_df = pd.merge(original_live_essential, intern_checking_file, how='left', on=merge_column, validate='1:m').fillna(value='None').drop_duplicates(subset=merge_column, keep='last')

    updated_df = merge_df[['recheck_id', 'track_title_no', 'itunes_id', 'album_region', 'itunes_album_link', 'version', 'artist_cover']]
    column_name = ['status', 'tracknum', 'ituneid', 'region', 'ituneurl', 'action_type', 'artist_cover']
    list_result = updated_df.values.tolist()  # transfer data_frame to 2D list
    list_result.insert(0, column_name)
    range_to_update = f"{sheet_name}!U1"
    update_value(list_result, range_to_update, gsheet_id)  # validate_value type: object, int, category... NOT DATETIME


def get_uuid_for_artist_name():
    artist_name = original_live_essential['artist_of_the_collection'].drop_duplicates(keep='first').tolist()

    df_artist = get_df_from_query(get_uuid_and_count_from_artist_name(artist_name)).astype(str).applymap(str.lower)

    original_artist_name = original_live_essential['artist_of_the_collection'].drop_duplicates(keep='first')
    original_artist_name = pd.merge(original_artist_name, df_artist, how='left', left_on='artist_of_the_collection',
                                    right_on='artist_name')
    return original_artist_name


def get_uuid_for_user_name():
    user_name = original_live_essential['user_name'].drop_duplicates(keep='first').tolist()
    df_user_name = get_df_from_query(get_user_uuid_from_user_name(user_name)).applymap(str.lower)

    original_user_name = original_live_essential['user_name'].drop_duplicates(keep='first')
    original_user_name = pd.merge(original_user_name, df_user_name, how='left', on='user_name')
    return original_user_name


def get_uuid_for_genre():
    genre_name = original_live_essential['genre_name'].drop_duplicates(keep='first').tolist()
    df_genre_name = get_df_from_query(get_genre_uuid_from_genre_name(genre_name)).applymap(str.lower)

    original_genre_name = original_live_essential['genre_name'].drop_duplicates(keep='first')
    original_genre_name = pd.merge(original_genre_name, df_genre_name, how='left', on='genre_name')
    return original_genre_name


def check_youtube_url():
    original_live_essential['len'] = original_live_essential['youtube_url'].apply(lambda x: len(x))
    original_live_essential['count'] = original_live_essential.groupby('youtube_url')['youtube_url'].transform('count')
    youtube_url = original_live_essential[['youtube_url', 'len', 'count']]

    return youtube_url


def crawl_itune_album():
    filter_df = original_live_essential[(original_live_essential.status == 'ok')
                                        & (original_live_essential.ituneid.notnull())
                                        & (original_live_essential.ituneid != '')
                                        & (original_live_essential.tracknum != '')
                                        & (original_live_essential.tracknum.notnull())
                                        ].reset_index().drop_duplicates(subset=['ituneid', 'region'],
                                                                        keep='first')  # remove duplicate df by column (reset_index before drop_duplicate: because of drop_duplicate default reset index)

    row_index = filter_df.index
    with open(query_path, "w") as f:
        for i in row_index:
            ituneid = filter_df.ituneid.loc[i]
            region = filter_df['region'].loc[i]
            query = f"insert into crawlingtasks(Id, ActionId, TaskDetail, Priority) values (uuid4(), '9C8473C36E57472281A1C7936108FC06',JSON_SET(IFNULL(crawlingtasks.TaskDetail, JSON_OBJECT()), '$.album_id','{ituneid}','$.region','{region}','$.PIC', \"Joy_xinh\"),999);\n"
            print(query)
            f.write(query)


def check_crawl_E5_06_status():
    # commit_message = input(f"\n Do you complete crawling_tasks insertion ?: True or False:")

    filter_df = original_live_essential[(original_live_essential.status == 'ok')
                                        & (original_live_essential.ituneid.notnull())
                                        & (original_live_essential.ituneid != '')
                                        & (original_live_essential.tracknum != '')
                                        & (original_live_essential.tracknum.notnull())
                                        ].reset_index().drop_duplicates(subset=['ituneid', 'region'], keep='first')
    ituneid = filter_df.ituneid.astype('int64').tolist()
    crawl_E5_06_status = get_df_from_query(get_crawl_E5_06_status(ituneid))
    # print(crawl_E5_06_status)
    return crawl_E5_06_status


def check_get_trackid_from_ituneid_and_tracknum():
    filter_df = original_live_essential[(original_live_essential.status == 'ok')
                                        & (original_live_essential.ituneid.notnull())
                                        & (original_live_essential.ituneid != '')
                                        & (original_live_essential.tracknum != '')
                                        & (original_live_essential.tracknum.notnull())
                                        ].reset_index().drop_duplicates(subset=['ituneid', 'tracknum'], keep='first')

    ituneid = filter_df['ituneid'].drop_duplicates().tolist()
    track_num = filter_df['tracknum'].drop_duplicates().tolist()
    trackid_from_ituneid_and_tracknum = get_df_from_query(
        get_trackid_from_ituneid_and_tracknum(ituneid, track_num)).astype({'tracknum': 'str'})

    trackid_from_ituneid_and_tracknum = pd.merge(filter_df[['status', 'ituneid', 'tracknum', 'single_title']],
                                                 trackid_from_ituneid_and_tracknum, how='left',
                                                 on=['ituneid', 'tracknum']).fillna(value='None')

    trackid_from_ituneid_and_tracknum['token_set_ratio'] = trackid_from_ituneid_and_tracknum.apply(
        lambda x: get_token_set_ratio(x.single_title, x.track_title), axis=1)
    # print(trackid_from_ituneid_and_tracknum)

    # Step print recheck_trackid_by_title
    joy_xinh = pd.merge(original_live_essential[['ituneid', 'tracknum']], trackid_from_ituneid_and_tracknum, how='left',
                        on=['ituneid', 'tracknum']).fillna(value='None')
    joy_xinh = joy_xinh[['track_id', 'track_title', 'track_artist', 'token_set_ratio']]
    column_name = joy_xinh.columns.values.tolist()
    list_result = joy_xinh.values.tolist()  # transfer data_frame to 2D list
    list_result.insert(0, column_name)
    range_to_update = f"{sheet_name}!AC1"
    update_value(list_result, range_to_update, gsheet_id)  # validate_value type: object, int, category... NOT DATETIME

    return trackid_from_ituneid_and_tracknum


def crawl_live_essential_youtube():
    '''
    Note: youtube_url trong crawler có phân biệt viết hoa viết thường, nên ko transform khi crawl
    :return:
    '''
    raw_live_essential = get_df_from_speadsheet(gsheet_id, sheet_name).apply(lambda x: x.str.strip())
    raw_live_essential = raw_live_essential.loc[:, ~raw_live_essential.columns.duplicated()]  # remove duplicate column
    filter_df = raw_live_essential[(raw_live_essential.status == 'ok')
                                   & (raw_live_essential.ituneid.notnull())
                                   & (raw_live_essential.ituneid != '')
                                   & (raw_live_essential.tracknum != '')
                                   & (raw_live_essential.tracknum.notnull())
                                   & (raw_live_essential.track_id != '')
                                   & (raw_live_essential.track_id.notnull())
                                   ]
    row_index = filter_df.index

    with open(query_path, "w") as f:
        for i in row_index:
            id = get_uuid4()
            youtube_url = filter_df.youtube_url.loc[i]
            # print(s.translate(str.maketrans({'o': 'O', 't': 'T'})))

            place = filter_df.live_festival_not_include_date.loc[i].translate(
                str.maketrans({'\"': '\\\"', '\'': '\\\''}))
            year = filter_df.year.loc[i]
            artist_cover = filter_df.artist_cover.loc[i].translate(str.maketrans({'\"': '\\\"', '\'': '\\\''}))
            action_type = filter_df.action_type.loc[i]
            trackid = filter_df.track_id.loc[i]

            if action_type == "live_video":
                query = f"insert into crawlingtasks(Id,ObjectID ,ActionId, TaskDetail, Priority) values ('{id}','{trackid}' ,'F91244676ACD47BD9A9048CF2BA3FFC1',JSON_SET(IFNULL(crawlingtasks.TaskDetail, JSON_OBJECT()),'$.when_exists','keep both' ,'$.youtube_url','{youtube_url}','$.data_source_format_id','7F8B6CD82F28437888BD029EFDA1E57F','$.concert_live_name','{place}','$.year','{year}','$.PIC', \"Joy_xinh\"),999);\n"
            elif action_type == "cover_video":
                query = f"insert into crawlingtasks(Id,ObjectID ,ActionId, TaskDetail, Priority) values ('{id}','{trackid}' ,'F91244676ACD47BD9A9048CF2BA3FFC1',JSON_SET(IFNULL(crawlingtasks.TaskDetail, JSON_OBJECT()),'$.when_exists','keep both' ,'$.youtube_url','{youtube_url}','$.data_source_format_id','F5D2FE4A15FB405E988A7309FD3F9920','$.covered_artist_name','{artist_cover}','$.year','{year}','$.PIC', \"Joy_xinh\"),999);\n"
            elif action_type == "fancam_video":
                query = f"insert into crawlingtasks(Id,ObjectID ,ActionId, TaskDetail, Priority) values ('{id}','{trackid}' ,'F91244676ACD47BD9A9048CF2BA3FFC1',JSON_SET(IFNULL(crawlingtasks.TaskDetail, JSON_OBJECT()),'$.when_exists','keep both' ,'$.youtube_url','{youtube_url}','$.data_source_format_id','DDC08421CAEB4E918F3FE373209020F9','$.concert_live_name','{place}','$.year','{year}','$.PIC', \"Joy_xinh\"),999);\n"
            else:
                continue
            f.write(query)


def get_datasourceid():
    raw_live_essential = get_df_from_speadsheet(gsheet_id, sheet_name).apply(lambda x: x.str.strip())

    get_datasourceid = get_df_from_query(get_datasourceId_from_crawlingtask()).apply(lambda s: s.str.replace('"', ""))

    get_datasourceid = pd.merge(raw_live_essential[['status', 'track_id', 'youtube_url']],
                                get_datasourceid, how='left',
                                on=['track_id', 'youtube_url']).fillna(value='None')

    get_datasourceid = get_datasourceid[['format_id', 'datasource_id', 'crawler_status', 'crawlingtask_id', 'message']]

    # print(get_datasourceid)
    # if 'datasource_id' in raw_live_essential.columns:
    #     pass
    # else:
    column_name = get_datasourceid.columns.values.tolist()
    list_result = get_datasourceid.values.tolist()  # transfer data_frame to 2D list
    list_result.insert(0, column_name)
    range_to_update = f"{sheet_name}!AG1"
    update_value(list_result, range_to_update,
                 gsheet_id)  # validate_value type: object, int, category... NOT DATETIME

    return get_datasourceid


def finalize_data():
    raw_live_essential = get_df_from_speadsheet(gsheet_id, sheet_name).fillna(value='None').apply(
        lambda x: x.str.strip())

    # artist_uuid
    artist_uuid = pd.merge(original_live_essential.artist_of_the_collection, get_uuid_for_artist_name(), how='left',
                           on='artist_of_the_collection').fillna(value='None')
    artist_uuid['artist_uuid'] = artist_uuid['artist_uuid'].str.upper()
    artist_uuid = artist_uuid[['artist_of_the_collection', 'artist_uuid']]

    # genr_uuid
    genre_uuid = pd.merge(original_live_essential.genre_name, get_uuid_for_genre(), how='left', on='genre_name').fillna(
        value='None')
    genre_uuid['genre_uuid'] = genre_uuid['genre_uuid'].str.upper()

    # user_uuid
    user_uuid = pd.merge(original_live_essential.user_name, get_uuid_for_user_name(), how='left',
                         on='user_name').fillna(value='None')
    user_uuid['user_uuid'] = user_uuid['user_uuid'].str.upper()

    # Playlist_id
    df_playlist_id = original_live_essential.drop_duplicates(subset='artist_of_the_collection',
                                                             keep='first').reset_index(drop=True)
    df_playlist_id['playlist_id'] = df_playlist_id.apply(lambda x: get_uuid4(),
                                                         axis=1)  # get value for each row with pandas
    df_playlist_id = df_playlist_id[['playlist_id', 'artist_of_the_collection']]
    df_playlist_id['PlaylistName'] = "Live Essentials : " + df_playlist_id['artist_of_the_collection'].str.title()
    df_playlist_id = pd.merge(original_live_essential.artist_of_the_collection, df_playlist_id, how='left',
                              on='artist_of_the_collection').fillna(value='None')

    joy = pd.concat([genre_uuid, artist_uuid, user_uuid, df_playlist_id, raw_live_essential], axis=1)
    joy = joy[['user_uuid', 'artist_uuid', 'genre_uuid', 'PlaylistName', 'playlist_id', 'datasource_id', 'youtube_url',
               'live_festival_not_include_date', 'rank', 'year', 'stadium_place', 'country', 'country', 'tempo']]
    # print(joy)

    column_name = joy.columns.values.tolist()
    list_result = joy.values.tolist()  # transfer data_frame to 2D list
    list_result.insert(0, column_name)
    range_to_update = f"{sheet_name}!AL1"
    update_value(list_result, range_to_update, gsheet_id)  # validate_value type: object, int, category... NOT DATETIME


def check_box():
    # Check all element:
    artist_name = get_uuid_for_artist_name()[(get_uuid_for_artist_name().count_uuid_by_name > '1')
                                             | (get_uuid_for_artist_name().artist_uuid.isnull())
                                             ]
    artist_name = artist_name[['artist_of_the_collection', 'count_uuid_by_name']].values.tolist()
    user_name = get_uuid_for_user_name()[(get_uuid_for_user_name().user_uuid.isnull())].to_numpy().tolist()
    genre_name = get_uuid_for_genre()[(get_uuid_for_genre().genre_uuid.isnull())].to_numpy().tolist()
    youtube_url = check_youtube_url()[(check_youtube_url()['len'] != 43)
                                      | (check_youtube_url()['count'] != 1)].to_numpy().tolist()
    crawl_E5_06_status = check_crawl_E5_06_status()[(check_crawl_E5_06_status()['06_status'] != 'complete')
                                                    | (check_crawl_E5_06_status()[
                                                           'E5_status'] != 'complete')].values.tolist()

    trackid_from_ituneid_and_tracknum = check_get_trackid_from_ituneid_and_tracknum()[
        (check_get_trackid_from_ituneid_and_tracknum().token_set_ratio < 90.0)].to_numpy().tolist()

    check_crawl_youtube = get_datasourceid()[(get_datasourceid().datasource_id == 'None')
                                             & (get_datasourceid().crawlingtask_id != 'None')
                                             ].values.tolist()

    # Convert checking element result to df:
    items = []
    status = []
    comment = []

    dict_value = [artist_name, user_name, genre_name, youtube_url, crawl_E5_06_status,
                  trackid_from_ituneid_and_tracknum, check_crawl_youtube]
    dict_key = ['artist_name', 'user_name', 'genre_name', 'youtube_url', 'crawl_E5_06_status',
                'trackid_from_ituneid_and_tracknum', 'check_crawl_youtube']
    dict_result = dict(zip(dict_key, dict_value))
    for i, j in dict_result.items():
        if not j:
            status.append('ok')
            comment.append(None)
            items.append(i)
        else:
            status.append('not ok')
            comment.append(j)
            items.append(i)
    print(comment)

    d = {'items': items, 'status': status, 'comment': comment}
    df = pd.DataFrame(data=d)
    print(df)

def update_date_live_essential():
    df = get_df_from_speadsheet(gsheet_id, sheet_name)[
        ['status', 'user_uuid', 'artist_uuid', 'genre_uuid', 'PlaylistName', 'playlist_id', 'datasource_id',
         'youtube_url', 'live_festival_not_include_date', 'rank', 'year', 'stadium_place', 'country', 'city',
         'tempo']]
    df = df.loc[:, ~df.columns.duplicated()]  # remove duplicate column
    filter_df = df[(df.status == "ok") & (df.datasource_id != "None")]
    essential_datasource = filter_df[
        ['playlist_id', 'datasource_id', 'youtube_url', 'live_festival_not_include_date', 'rank', 'year',
         'stadium_place', 'country', 'city', 'tempo']]
    essential_datasource_row_index = essential_datasource.index

    essential_playlist = filter_df[
        ['playlist_id', 'user_uuid', 'artist_uuid', 'PlaylistName', 'genre_uuid', 'rank']].drop_duplicates(
        subset='playlist_id', keep="first")
    essential_playlist_row_index = essential_playlist.index
    query = ""
    with open(query_path, "w") as f:
        for i in essential_playlist_row_index:
            playlist_id = essential_playlist.playlist_id.loc[i]
            user_uuid = essential_playlist.user_uuid.loc[i]
            artist_uuid = essential_playlist.artist_uuid.loc[i]
            PlaylistName = essential_playlist.PlaylistName.loc[i].translate(str.maketrans({'\"': '\\\"', '\'': '\\\''}))
            genre_uuid = essential_playlist.genre_uuid.loc[i]
            rank = essential_playlist['rank'].loc[i]
            query = query + f"INSERT into essential_playlist (`Id`,`UserId`,`ArtistId`,`PlaylistName`,`GenreId`,`Rank`, `ThumbnailRank`) VALUES ('{playlist_id}','{user_uuid}','{artist_uuid}','{PlaylistName}','{genre_uuid}',{rank},{rank});\n"

        for j in essential_datasource_row_index:
            playlist_id = essential_datasource.playlist_id.loc[j]
            datasource_id = essential_datasource.datasource_id.loc[j]
            youtube_url = essential_datasource.youtube_url.loc[j]
            festival = essential_datasource.live_festival_not_include_date.loc[j].translate(
                str.maketrans({'\"': '\\\"', '\'': '\\\''}))
            rank = essential_datasource['rank'].loc[j]
            year = essential_datasource['year'].loc[j]
            stadium_place = essential_datasource.stadium_place.loc[j].translate(
                str.maketrans({'\"': '\\\"', '\'': '\\\''}))
            country = essential_datasource.country.loc[j].translate(str.maketrans({'\"': '\\\"', '\'': '\\\''}))
            city = essential_datasource.city.loc[j].translate(str.maketrans({'\"': '\\\"', '\'': '\\\''}))
            tempo = essential_datasource.tempo.loc[j].translate(str.maketrans({'\"': '\\\"', '\'': '\\\''}))
            query = query + f"INSERT into essential_playlist_datasource (`PlaylistId`,`DatasourceId`,`SourceURI`,`Festival`,`Rank`,`LiveDate`,`StadiumPlace`,`Country`,`StateCity`,`Tempo`) VALUES ('{playlist_id}','{datasource_id}','{youtube_url}','{festival}','{rank}','{year}','{stadium_place}','{country}','{city}','{tempo}');\n"
        print(query)
        f.write(query)


if __name__ == "__main__":
    pd.set_option("display.max_rows", None, "display.max_columns", 80, 'display.width', 1000)
    start_time = time.time()
    # INPUT HERE:
    # Input_url 'https://docs.google.com/spreadsheets/d/1uK18IYVtUv-_xXSuossOdLZkrMwRT_49mz9oVLT4DUg/edit#gid=892369772'
    gsheet_id = '1uK18IYVtUv-_xXSuossOdLZkrMwRT_49mz9oVLT4DUg'  # Single page
    sheet_name = 'Nov - W2'
    original_live_essential = get_df_from_speadsheet(gsheet_id, sheet_name).fillna(value='None').applymap(
        str.lower).apply(lambda x: x.str.strip())

    # PROCESS HERE:
    # intern_checking_process()
    # loop_data_from_intern_process()
    # check_box()
    # crawl_itune_album()

    # check_crawl_E5_06_status()
    check_get_trackid_from_ituneid_and_tracknum()
    # crawl_live_essential_youtube()

    # get_datasourceid()
    # finalize_data()
    # update_date_live_essential()
    print("\n --- total time to process %s seconds ---" % (time.time() - start_time))
