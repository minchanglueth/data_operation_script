from google_spreadsheet_api.function import get_df_from_speadsheet, get_list_of_sheet_title, update_value, \
    creat_new_sheet_and_update_data_from_df, get_gsheet_name
from google_spreadsheet_api.create_new_sheet_and_update_data_from_df import creat_new_sheet_and_update_data_from_df

from core.crud.sql.datasource import get_datasourceid_from_youtube_url_and_trackid, related_datasourceid, \
    get_youtube_info_from_trackid
from core.models.data_source_format_master import DataSourceFormatMaster
from core.crud.get_df_from_query import get_df_from_query
from core.crud.sql.crawlingtask import get_artist_image_cant_crawl, get_album_image_cant_crawl

from youtube_dl_fuction.fuctions import get_raw_title_uploader_from_youtube_url
from support_function.automate_checking_crawler import automate_check_crawl_image_status

from tools import get_uuid4
from itertools import chain
import pandas as pd
import time
from core import query_path
from support_function.text_similarity.text_similarity import get_token_set_ratio
from numpy import random
import numpy as np


# gsheet_id = input(f"\n Input gsheet_id: ").strip()

class sheet_type:
    MP3_SHEET_NAME = {"sheet_name": "MP_3", "fomatid": DataSourceFormatMaster.FORMAT_ID_MP3_FULL,
                      "column_name": ["track_id", "Memo", "Mp3_link", "url_to_add"]}
    MP4_SHEET_NAME = {"sheet_name": "MP_4", "fomatid": DataSourceFormatMaster.FORMAT_ID_MP4_FULL,
                      "column_name": ["track_id", "Memo", "MP4_link", "url_to_add"]}
    VERSION_SHEET_NAME = {"sheet_name": "Version_done", "fomatid": [DataSourceFormatMaster.FORMAT_ID_MP4_REMIX,
                                                                    DataSourceFormatMaster.FORMAT_ID_MP4_LIVE],
                          "column_name": ["track_id", "Remix_url", "Remix_artist", "Live_url", "Live_venue",
                                          "Live_year"]}

    ARTIST_IMAGE = {"sheet_name": "Artist_image", "column_name": ["Artist_uuid", "Memo", "url_to_add"], "object_type": "artist"}
    ALBUM_IMAGE = {"sheet_name": "Album_image", "column_name": ["Album_uuid", "Memo", "url_to_add"], "object_type": "album"}

    ARTIST_WIKI = {"sheet_name": "Artist_wiki", "column_name": ["Artist_uuid", "Memo", "url_to_add", "content to add"],
                   "table_name": "artists"}
    ALBUM_WIKI = {"sheet_name": "Album_wiki", "column_name": ["Album_uuid", "Memo", "url_to_add", "Content_to_add"],
                  "table_name": "albums"}


def check_youtube_url_mp3():
    '''
    TrackID	Memo	URL_to_add	Type	Assignee
                                        no need to check
    not null	added	length = 43	    C/D/Z
    not null	not found	none	none
    :return:
    '''
    sheet_name = 'MP_3'
    original_df = get_df_from_speadsheet(gsheet_id, sheet_name).applymap(str.lower)

    original_df['len'] = original_df['url_to_add'].apply(lambda x: len(x))
    youtube_url_mp3 = original_df[['track_id', 'Memo', 'url_to_add', 'len', 'Type', 'Assignee']]

    check_youtube_url_mp3 = youtube_url_mp3[~
    ((
             (youtube_url_mp3['track_id'] != '')
             & (youtube_url_mp3['Memo'] == 'added')
             & (youtube_url_mp3['len'] == 43)
             & (youtube_url_mp3['Type'].isin(["c", "d", "z"]))
     ) |
     (
             (youtube_url_mp3['track_id'] != '')
             & (youtube_url_mp3['Memo'] == 'not found')
             & (youtube_url_mp3['url_to_add'] == 'none')
             & (youtube_url_mp3['Type'] == 'none')
     ) |
     (

         (youtube_url_mp3['Assignee'] == 'no need to check')
     ))
    ]

    return check_youtube_url_mp3.track_id.str.upper()


def check_youtube_url_mp4():
    '''
        TrackID	Memo	URL_to_add	Assignee
                                    no need to check
        not null	ok	null
        not null	added	length = 43
        not null	not found	none
        not null	not ok	length = 43
        not null	not ok	none
    :return:
    '''

    sheet_name = 'MP_4'
    original_df = get_df_from_speadsheet(gsheet_id, sheet_name).applymap(str.lower)
    original_df['len'] = original_df['url_to_add'].apply(lambda x: len(x))
    youtube_url_mp4 = original_df[['track_id', 'Memo', 'url_to_add', 'len', 'Assignee']]

    check_youtube_url_mp4 = youtube_url_mp4[~
    ((
             (youtube_url_mp4['track_id'] != '')
             & (youtube_url_mp4['Memo'] == 'ok')
             & (youtube_url_mp4['url_to_add'] == '')
     ) |
     (
             (youtube_url_mp4['track_id'] != '')
             & (youtube_url_mp4['Memo'] == 'added')
             & (youtube_url_mp4['len'] == 43)
     ) |
     (
             (youtube_url_mp4['track_id'] != '')
             & (youtube_url_mp4['Memo'] == 'not found')
             & (youtube_url_mp4['url_to_add'] == 'none')
     ) |
     (
             (youtube_url_mp4['track_id'] != '')
             & (youtube_url_mp4['Memo'] == 'not ok')
             & (youtube_url_mp4['len'] == 43)
     ) |
     (
             (youtube_url_mp4['track_id'] != '')
             & (youtube_url_mp4['Memo'] == 'not ok')
             & (youtube_url_mp4['url_to_add'] == 'none')
     ) |
     (youtube_url_mp4['Assignee'] == 'no need to check')
     )
    ]
    return check_youtube_url_mp4.track_id.str.upper()


def check_version():
    '''
    TrackID	    URL         	Remix_Artist
    not null	length = 43	    not null
    not null	null	        null

    TrackID	    URL2	        Venue           Live_year
    not null	length = 43	    not null        null hoặc nằm trong khoảng từ 1950 đến 2030
    not null	null	        null            null
    '''

    sheet_name = 'Version_done'
    original_df = get_df_from_speadsheet(gsheet_id, sheet_name).applymap(str.lower)

    original_df['len_remix_url'] = original_df['Remix_url'].apply(lambda x: len(x))
    original_df['len_live_url'] = original_df['Live_url'].apply(lambda x: len(x))
    original_df['Live_year'] = pd.to_numeric(original_df.Live_year, errors='coerce').astype('Int64').fillna(0)

    youtube_url_version = original_df[
        ['track_id', 'Remix_url', 'Remix_artist', 'Live_url', 'Live_venue', 'len_remix_url', 'len_live_url',
         'Live_year']]

    check_version = youtube_url_version[~
    (((
              (youtube_url_version['track_id'] != '')
              & (youtube_url_version['len_remix_url'] == 43)
              & (youtube_url_version['Remix_artist'] != '')
      ) |
      (
              (youtube_url_version['track_id'] != '')
              & (youtube_url_version['Remix_url'] == '')
              & (youtube_url_version['Remix_artist'] == '')
      )) &
     ((
              (youtube_url_version['track_id'] != '')
              & (youtube_url_version['len_live_url'] == 43)
              & (youtube_url_version['Live_venue'] != '')
              & ((youtube_url_version['Live_year'] == 0) | (
              (1950 <= original_df['Live_year']) & (original_df['Live_year'] <= 2030)))
      ) |
      (
              (youtube_url_version['track_id'] != '')
              & (youtube_url_version['Live_url'] == '')
              & (youtube_url_version['Live_venue'] == '')
              & (youtube_url_version['Live_year'] == 0)
      )))
    ]
    return check_version.track_id.str.upper()


def check_album_image():
    '''
        AlbumUUID	Memo	URL_to_add	Assignee
                                        no need to check
        not null	ok	null
        not null	added	not null
        not null	not found	none
    :return:
    '''
    sheet_name = 'Album_image'
    original_df = get_df_from_speadsheet(gsheet_id, sheet_name).applymap(str.lower)
    album_image = original_df[['Album_uuid', 'Memo', 'url_to_add', 'Assignee']]

    check_album_image = album_image[~
    ((
             (album_image['Album_uuid'] != '')
             & (album_image['Memo'] == 'ok')
             & (album_image['url_to_add'] == '')
     ) |
     (
             (album_image['Album_uuid'] != '')
             & (album_image['Memo'] == 'added')
             & (album_image['url_to_add'] != '')
     ) |
     (
             (album_image['Album_uuid'] != '')
             & (album_image['Memo'] == 'not found')
             & (album_image['url_to_add'] == 'none')
     ) |
     (
         (album_image['Assignee'] == 'no need to check')
     ))
    ]

    return check_album_image.Album_uuid.str.upper()


def check_artist_image():
    '''
        ArtistTrackUUID	Memo	URL_to_add	Assignee
                                            no need to check
        not null	ok	null
        not null	added	not null
    :return:
    '''
    sheet_name = 'Artist_image'
    original_df = get_df_from_speadsheet(gsheet_id, sheet_name).applymap(str.lower)
    artist_image = original_df[['Artist_uuid', 'Memo', 'url_to_add', 'Assignee']]

    check_artist_image = artist_image[~
    ((
             (artist_image['Artist_uuid'] != '')
             & (artist_image['Memo'] == 'ok')
             & (artist_image['url_to_add'] == '')
     ) |
     (
             (artist_image['Artist_uuid'] != '')
             & (artist_image['Memo'] == 'added')
             & (artist_image['url_to_add'] != '')
     ) |
     (
         (artist_image['Assignee'] == 'no need to check')
     ))
    ]
    return check_artist_image.Artist_uuid.str.upper()


def check_album_wiki():
    '''
        AlbumUUID	Memo	URL_to_add	Content_to_add	Assignee
                                                        no need to check
        not null	ok	null	null
        not null	added	https://en.wikipedia.org/%	not null
        not null	not found	none	none
        not null	not ok	https://en.wikipedia.org/%	not null
        not null	not ok	none	none
    :return:
    '''
    sheet_name = 'Album_wiki'
    original_df = get_df_from_speadsheet(gsheet_id, sheet_name).applymap(str.lower)
    album_wiki = original_df[['Album_uuid', 'Memo', 'url_to_add', 'Content_to_add', 'Assignee']]

    check_album_wiki = album_wiki[~
    ((
             (album_wiki['Album_uuid'] != '')
             & (album_wiki['Memo'] == 'ok')
             & (album_wiki['url_to_add'] == '')
             & (album_wiki['Content_to_add'] == '')
     ) |
     (
             (album_wiki['Album_uuid'] != '')
             & (album_wiki['Memo'] == 'added')
             & (album_wiki['url_to_add'].str[:25] == 'https://en.wikipedia.org/')
             & (album_wiki['Content_to_add'] != '')
     ) |
     (
             (album_wiki['Album_uuid'] != '')
             & (album_wiki['Memo'] == 'not found')
             & (album_wiki['url_to_add'] == 'none')
             & (album_wiki['Content_to_add'] == 'none')
     ) |
     (
             (album_wiki['Album_uuid'] != '')
             & (album_wiki['Memo'] == 'not ok')
             & (album_wiki['url_to_add'].str[:25] == 'https://en.wikipedia.org/')
             & (album_wiki['Content_to_add'] != '')
     ) |
     (
             (album_wiki['Album_uuid'] != '')
             & (album_wiki['Memo'] == 'not ok')
             & (album_wiki['url_to_add'] == 'none')
             & (album_wiki['Content_to_add'] == 'none')
     ) |
     (
         (album_wiki['Assignee'] == 'no need to check')
     ))
    ]
    return check_album_wiki.Album_uuid.str.upper()


def check_artist_wiki():
    '''
        Artist_uuid	Memo	URL_to_add	Assignee
                    no need to check
        not null	ok	null
        not null	added	https://en.wikipedia.org/%
        not null	not found	none
        not null	not ok	https://en.wikipedia.org/%
        not null	not ok	none
    :return:
    '''

    sheet_name = 'Artist_wiki'
    original_df = get_df_from_speadsheet(gsheet_id, sheet_name).applymap(str.lower)
    artist_wiki = original_df[['Artist_uuid', 'Memo', 'url_to_add', 'Assignee']]

    check_artist_wiki = artist_wiki[~
    ((
             (artist_wiki['Artist_uuid'] != '')
             & (artist_wiki['Memo'] == 'ok')
             & (artist_wiki['url_to_add'] == '')
     ) |
     (
             (artist_wiki['Artist_uuid'] != '')
             & (artist_wiki['Memo'] == 'added')
             & (artist_wiki['url_to_add'].str[:25] == 'https://en.wikipedia.org/')
     ) |
     (
             (artist_wiki['Artist_uuid'] != '')
             & (artist_wiki['Memo'] == 'not found')
             & (artist_wiki['url_to_add'] == 'none')
     ) |
     (
             (artist_wiki['Artist_uuid'] != '')
             & (artist_wiki['Memo'] == 'not ok')
             & (artist_wiki['url_to_add'].str[:25] == 'https://en.wikipedia.org/')
     ) |
     (
             (artist_wiki['Artist_uuid'] != '')
             & (artist_wiki['Memo'] == 'not ok')
             & (artist_wiki['url_to_add'] == 'none')
     ) |
     (
         (artist_wiki['Assignee'] == 'no need to check')
     ))
    ]
    return check_artist_wiki.Artist_uuid.str.upper()


def check_box():
    # Check all element:
    list_of_sheet_title = get_list_of_sheet_title(gsheet_id)

    if 'MP_3' in list_of_sheet_title:
        youtube_url_mp3 = check_youtube_url_mp3().to_numpy().tolist()
    else:
        youtube_url_mp3 = ["MP_3 not found"]

    if 'MP_4' in list_of_sheet_title:
        youtube_url_mp4 = check_youtube_url_mp4().to_numpy().tolist()
    else:
        youtube_url_mp4 = ["MP_4 not found"]

    if 'Version_done' in list_of_sheet_title:
        version = check_version().to_numpy().tolist()
    else:
        version = ["Version_done not found"]

    if 'Album_image' in list_of_sheet_title:
        album_image = check_album_image().to_numpy().tolist()
    else:
        album_image = ["Album_image not found"]

    if 'Artist_image' in list_of_sheet_title:
        artist_image = check_artist_image().to_numpy().tolist()
    else:
        artist_image = ["Artist_image not found"]

    if 'Album_wiki' in list_of_sheet_title:
        album_wiki = check_album_wiki().to_numpy().tolist()
    else:
        album_wiki = ["Album_wiki not found"]

    if 'Artist_wiki' in list_of_sheet_title:
        artist_wiki = check_artist_wiki().to_numpy().tolist()
    else:
        artist_wiki = ["Artist_wiki not found"]

    # Convert checking element result to df:
    items = []
    status = []
    comment = []

    dict_value = [youtube_url_mp3, youtube_url_mp4, version, album_image, artist_image, album_wiki, artist_wiki]
    dict_key = ['youtube_url_mp3', 'youtube_url_mp4', 'version', 'album_image', 'artist_image', 'album_wiki',
                'artist_wiki']
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
    df = pd.DataFrame(data=d).astype(str)
    print(df)

    creat_new_sheet_and_update_data_from_df(df, gsheet_id, 'jane_to_check_result')

    return df


def process_mp3_mp4(sheet_info: dict):
    '''
    Memo = not ok and url_to_add = 'none'
        => if not existed in related_table: set valid = -10
        => if existed in related_table:
            - set trackid = blank and formatid = blank
            - update trackcountlogs
    Memo = not ok and url_to_add not null => crawl mode replace
    Memo = added => crawl mode skip
    '''
    k = check_box()
    check_box_filtered = k[~(
            (k['status'] == "not ok")
            & (k['comment'].str.contains('not found')))
    ]
    checking = 'not ok' in check_box_filtered.status.drop_duplicates().tolist()
    if checking == 1:
        return print("Please recheck check_box")
    else:
        sheet_name = sheet_info['sheet_name']
        original_df = get_df_from_speadsheet(gsheet_id, sheet_name)
        youtube_url = original_df[sheet_info['column_name']]
        file_to_process = youtube_url[
            (youtube_url['Memo'] == 'not ok') | (youtube_url['Memo'] == 'added')].reset_index().drop_duplicates(
            subset=['track_id'],
            keep='first')  # remove duplicate df by column (reset_index before drop_duplicate: because of drop_duplicate default reset index)

        row_index = file_to_process.index
        old_youtube_url_column_name = sheet_info['column_name'][2]
        datasource_format_id = sheet_info['fomatid']
        with open(query_path, "w") as f:
            for i in row_index:
                memo = file_to_process.Memo.loc[i]
                new_youtube_url = file_to_process.url_to_add.loc[i]
                track_id = file_to_process.track_id.loc[i]
                old_youtube_url = file_to_process[old_youtube_url_column_name].loc[i]
                query = ""
                if memo == "not ok" and new_youtube_url == "none":
                    datasourceids = get_datasourceid_from_youtube_url_and_trackid(youtube_url=old_youtube_url,
                                                                                  trackid=track_id,
                                                                                  formatid=datasource_format_id).all()
                    datasourceids_flatten_list = tuple(set(list(chain.from_iterable(datasourceids))))  # flatten list
                    if not datasourceids_flatten_list:
                        continue
                    else:
                        for datasourceid in datasourceids_flatten_list:
                            related_id_datasource = related_datasourceid(datasourceid)
                            if related_id_datasource == [(None, None, None)]:
                                query = query + f"Update datasources set valid = -10 where id = {datasourceid};"
                            else:
                                query = query + f"UPDATE datasources SET trackid = '', FormatID = ''  where id = '{datasourceid}';\n"
                                query = query + f"UPDATE datasources SET updatedAt = NOW() WHERE trackid = '{track_id}';\n"

                elif memo == "not ok" and new_youtube_url != "none":
                    query = query + f"insert into crawlingtasks(Id,ObjectID ,ActionId, TaskDetail, Priority) values (uuid4(),'{track_id}' ,'F91244676ACD47BD9A9048CF2BA3FFC1',JSON_SET(IFNULL(crawlingtasks.TaskDetail, JSON_OBJECT()),'$.when_exists','replace' ,'$.youtube_url','{new_youtube_url}','$.data_source_format_id','{datasource_format_id}','$.PIC', '{gsheet_name}_{sheet_name}'),300);\n"
                elif memo == "added":
                    query = query + f"insert into crawlingtasks(Id,ObjectID ,ActionId, TaskDetail, Priority) values (uuid4(),'{track_id}' ,'F91244676ACD47BD9A9048CF2BA3FFC1',JSON_SET(IFNULL(crawlingtasks.TaskDetail, JSON_OBJECT()),'$.when_exists','skip' ,'$.youtube_url','{new_youtube_url}','$.data_source_format_id','{datasource_format_id}','$.PIC', '{gsheet_name}_{sheet_name}'),300);\n"
                f.write(query)


def process_version_sheet(sheet_info: dict):
    k = check_box()
    check_box_filtered = k[~(
            (k['status'] == "not ok")
            & (k['comment'].str.contains('not found')))
    ]
    checking = 'not ok' in check_box_filtered.status.drop_duplicates().tolist()
    if checking == 1:
        return print("Please recheck check_box")
    else:
        sheet_name = sheet_info['sheet_name']
        original_df = get_df_from_speadsheet(gsheet_id, sheet_name)
        youtube_url = original_df[sheet_info['column_name']]
        file_to_process = youtube_url[
            (youtube_url['Remix_url'] != '') | (youtube_url['Live_url'] != '')].reset_index().drop_duplicates(
            subset=['track_id', 'Remix_url', 'Live_url'],
            keep='first')  # remove duplicate df by column (reset_index before drop_duplicate: because of drop_duplicate default reset index)

        row_index = file_to_process.index
        with open(query_path, "a") as f:
            for i in row_index:
                Remix_url = file_to_process.Remix_url.loc[i]
                Remix_artist = file_to_process.Remix_artist.loc[i].replace('\'', '\\\'').replace("\"", "\\\"")
                track_id = file_to_process.track_id.loc[i]
                Live_url = file_to_process.Live_url.loc[i]
                Live_venue = file_to_process.Live_venue.loc[i].replace('\'', '\\\'').replace("\"", "\\\"")
                Live_year = file_to_process.Live_year.loc[i]
                query = ""
                if Remix_url != '':
                    query = query + f"insert into crawlingtasks(Id,ObjectID ,ActionId, TaskDetail, Priority) values (uuid4(),'{track_id}' ,'F91244676ACD47BD9A9048CF2BA3FFC1',JSON_SET(IFNULL(crawlingtasks.TaskDetail, JSON_OBJECT()),'$.when_exists','keep both' ,'$.youtube_url','{Remix_url}','$.data_source_format_id','{DataSourceFormatMaster.FORMAT_ID_MP4_REMIX}','$.remix_artist','{Remix_artist}','$.PIC', '{gsheet_name}_{sheet_name}'),300);\n"
                else:
                    pass
                if Live_url != '' and Live_year != '':
                    query = query + f"insert into crawlingtasks(Id,ObjectID ,ActionId, TaskDetail, Priority) values (uuid4(),'{track_id}' ,'F91244676ACD47BD9A9048CF2BA3FFC1',JSON_SET(IFNULL(crawlingtasks.TaskDetail, JSON_OBJECT()),'$.when_exists','keep both' ,'$.youtube_url','{Live_url}','$.data_source_format_id','{DataSourceFormatMaster.FORMAT_ID_MP4_LIVE}','$.concert_live_name','{Live_venue}','$.year','{Live_year}','$.PIC', '{gsheet_name}_{sheet_name}'),300);\n"
                elif Live_url != '' and Live_year == '':
                    query = query + f"insert into crawlingtasks(Id,ObjectID ,ActionId, TaskDetail, Priority) values (uuid4(),'{track_id}' ,'F91244676ACD47BD9A9048CF2BA3FFC1',JSON_SET(IFNULL(crawlingtasks.TaskDetail, JSON_OBJECT()),'$.when_exists','keep both' ,'$.youtube_url','{Live_url}','$.data_source_format_id','{DataSourceFormatMaster.FORMAT_ID_MP4_LIVE}','$.concert_live_name','{Live_venue}','$.PIC', '{gsheet_name}_{sheet_name}'),300);\n"
                else:
                    pass
                print(query)
                f.write(query)


def final_check():
    sheet_name = sheet_info['sheet_name']
    datasource_format_id = sheet_info['fomatid']
    original_sheet = get_df_from_speadsheet(gsheet_id, sheet_name)

    list_trackid = original_sheet['track_id'].drop_duplicates().values.tolist()
    query_df = get_df_from_query(get_youtube_info_from_trackid(list_trackid, datasource_format_id))

    merge_df = pd.merge(original_sheet, query_df, how='left',
                        on='track_id').fillna(value='None')
    merge_df = merge_df.head(100)

    row_index = merge_df.index
    get_youtube_titles = []
    get_youtube_uploaders = []
    token_set_ratio_titles = []
    token_set_ratio_artists = []

    for i in row_index:
        youtube_title = merge_df.youtube_title.loc[i]
        youtube_uploader = merge_df.youtube_uploader.loc[i]
        youtube_url = merge_df.youtube_url.loc[i]

        itune_title = merge_df['Song Title on Itunes'].loc[i]
        itune_artist = merge_df['Artist Track on iTunes'].loc[i]

        if "https://www.youtube.com/watch?v=" in youtube_url and (youtube_title == 'None' or youtube_title == "\"_\""):
            get_youtube_info = get_raw_title_uploader_from_youtube_url(youtube_url)
            get_youtube_title = get_youtube_info['youtube_title']
            get_youtube_uploader = get_youtube_info['uploader']

            get_youtube_titles.append(get_youtube_title)
            get_youtube_uploaders.append(get_youtube_uploader)

            token_set_ratio_title = get_token_set_ratio(itune_title, get_youtube_title)
            token_set_ratio_artist = get_token_set_ratio(itune_artist, get_youtube_uploader)

            token_set_ratio_titles.append(token_set_ratio_title)
            token_set_ratio_artists.append(token_set_ratio_artist)

            x = random.uniform(0.5, 5)
            time.sleep(x)

        elif "https://www.youtube.com/watch?v=" in youtube_url:
            get_youtube_titles.append(youtube_title)
            get_youtube_uploaders.append(youtube_uploader)

            token_set_ratio_title = get_token_set_ratio(itune_title, youtube_title)
            token_set_ratio_artist = get_token_set_ratio(itune_artist, youtube_uploader)

            token_set_ratio_titles.append(token_set_ratio_title)
            token_set_ratio_artists.append(token_set_ratio_artist)
        else:
            get_youtube_titles.append("None")
            get_youtube_uploaders.append("None")
            token_set_ratio_titles.append("None")
            token_set_ratio_artists.append("None")

    se_youtube_title = pd.Series(get_youtube_titles)
    se_youtube_uploader = pd.Series(get_youtube_uploaders)

    se_token_set_ratio_title = pd.Series(token_set_ratio_titles)
    se_token_set_ratio_artist = pd.Series(token_set_ratio_artists)

    merge_df['get_youtube_title'] = se_youtube_title.values
    merge_df['get_youtube_uploader'] = se_youtube_uploader.values
    merge_df['token_set_ratio_title'] = se_token_set_ratio_title.values
    merge_df['token_set_ratio_artist'] = se_token_set_ratio_artist.values

    # print(merge_df)

    updated_df = merge_df[['datasource_id', 'youtube_url', 'youtube_title', 'youtube_uploader', 'get_youtube_title',
                           'get_youtube_uploader', 'token_set_ratio_title', 'token_set_ratio_artist']]
    column_name = updated_df.columns.values.tolist()
    list_result = updated_df.values.tolist()  # transfer data_frame to 2D list
    list_result.insert(0, column_name)
    range_to_update = f"{sheet_name}!K1"
    update_value(list_result, range_to_update, gsheet_id)  # validate_value type: object, int, category... NOT DATETIME


def crawl_image(sheet_info: dict):
    '''
    ARTIST_IMAGE = {"sheet_name": "Artist_image", "column_name": ["Artist_uuid", "Memo", "url_to_add"]}
    ALBUM_IMAGE = {"sheet_name": "Album_image", "column_name": ["Album_uuid", "Memo", "url_to_add"]}
    :param sheet_info:
    :return:
    '''

    # Step 1: Select process sheet
    sheet_name = sheet_info['sheet_name']
    column_name = sheet_info['column_name']
    new_sheet_name = f"{sheet_name} cant upload"
    if new_sheet_name in list_of_sheet_title:
        filter_df = get_df_from_speadsheet(gsheet_id, new_sheet_name)
        print(f"List {sheet_name} to crawl image \n ", filter_df, "\n")
    else:
        df = get_df_from_speadsheet(gsheet_id, sheet_name)
        filter_df = df[(df.Memo == 'added')  # filter df by conditions
                       & (df.url_to_add.notnull())
                       & (df.url_to_add != '')
                       ].reset_index().drop_duplicates(subset=[column_name[0]],
                                                       keep='first')  # remove duplicate df by column (reset_index before drop_duplicate: because of drop_duplicate default reset index)
        print(f"List {sheet_name} to crawl image \n ", filter_df[column_name],
              "\n")

    # Step 2: get crawlingtask
    row_index = filter_df.index
    with open(query_path, "w") as f:
        for i in row_index:
            objectid = filter_df[column_name[0]].loc[i]
            url_to_add = filter_df[column_name[2]].loc[i]
            query = f"insert into crawlingtasks(Id, ActionId,objectid ,TaskDetail, Priority) values (uuid4(), 'OA9CPKSUT6PBGI1ZHPLQUPQCGVYQ71S9','{objectid}',JSON_SET(IFNULL(crawlingtasks.TaskDetail, JSON_OBJECT()), '$.url','{url_to_add}','$.object_type', {sheet_info['column_name']}, '$.when_exists',\"replace\",'$.PIC', '{gsheet_name}_{sheet_name}'),1999);"
            f.write(query + "\n")

    # Step 3: automation check crawl_artist_image_status then export result:
    # automate_check_crawl_image_status()

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
    #
    # creat_new_sheet_and_update_data_from_df(df_to_upload, gsheet_id, new_sheet_name)


def update_wiki_result_to_gsheet():  # both single page and album page
    sheet_name = 'wiki'
    df_wiki = get_df_from_speadsheet(gsheet_id, sheet_name)

    conditions = [  # create a list of condition => if true =>> update value tương ứng
        ((df_wiki['Memo'] == 'not ok') | (df_wiki['Memo'] == 'added')) & (df_wiki['content to add'] != 'none') & (
                df_wiki.url_to_add != 'none') & (df_wiki['content to add'] != '') & (df_wiki.url_to_add != ''),
        ((df_wiki['Memo'] == 'not ok') | (df_wiki['Memo'] == 'added')) & (
                (df_wiki['content to add'] == 'none') | (df_wiki.url_to_add == 'none') | (
                df_wiki['content to add'] == '') | (df_wiki.url_to_add == '')),
        True]
    values = ['wiki added', 'remove wiki', None]  # create a list of the values tương ứng với conditions ơ trên
    df_wiki['joy xinh'] = np.select(conditions,
                                    values)  # create a new column and use np.select to assign values to it using our lists as arguments
    column_title = ['Joy note']
    list_result = np.array(df_wiki['joy xinh']).reshape(-1,
                                                        1).tolist()  # Chuyển về list từ 1 chiều về 2 chiều sử dung Numpy
    list_result.insert(0, column_title)
    range_to_update = f"{sheet_name}!J"

    update_value(list_result, range_to_update, gsheet_id)
    print("update data in gsheet completed")


def update_wiki(sheet_info: dict):
    '''
    ARTIST_WIKI = {"sheet_name": "Artist_wiki", "column_name": ["Artist_uuid", "Memo", "url_to_add", "content to add"], "table_name": "artists"}
    ALBUM_WIKI = {"sheet_name": "Album_wiki", "column_name": ["Album_uuid", "Memo", "url_to_add", "content to add"], "table_name": "albums"}
    :param sheet_info:
    :return:
    '''

    sheet_name = sheet_info['sheet_name']
    df_wiki = get_df_from_speadsheet(gsheet_id, sheet_name)
    df_wiki_filter = df_wiki[(df_wiki.Memo == 'added') | (df_wiki.Memo == 'not ok')]
    row_index = df_wiki_filter.index
    column_name = sheet_info['column_name']
    table_name = sheet_info['table_name']
    with open(query_path, "w") as f:
        for i in row_index:
            uuid = df_wiki_filter[column_name[0]].loc[i]
            url = df_wiki_filter[column_name[2]].loc[i]
            content = df_wiki_filter[column_name[3]].loc[i].replace('\'', '\\\'').replace("\"", "\\\"")
            joy_xinh = f"Update {table_name} set info =  Json_replace(Json_remove(info,'$.wiki'),'$.wiki_url','not ok') where uuid = '{uuid}';"
            query = ""
            if url != "" and content != "" and url != 'none' and content != 'none':
                query = f"UPDATE {table_name} SET info = Json_set(if(info is null,JSON_OBJECT(),info), '$.wiki', JSON_OBJECT('brief', '{content}'), '$.wiki_url','{url}') WHERE uuid = '{uuid}';"
            else:
                query = query
            f.write(joy_xinh + "\n" + query + "\n")
            print(joy_xinh + "\n" + query + "\n")

    # Step 3: update gsheet
    # update_wiki_result_to_gsheet()


def get_gsheet_id_from_url(url: str):
    url_list = url.split("/")
    gsheet_id = url_list[5]
    return gsheet_id


if __name__ == "__main__":
    start_time = time.time()
    pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)
    # INPUT HERE
    # Justin requirement: https://docs.google.com/spreadsheets/d/1LClklcO0OEMmQ1iaCZ34n1hhjlP1lIBj7JMjm2qrYVw/edit#gid=0
    # Jane requirement: https://docs.google.com/spreadsheets/d/1nm7DRUX0v1zODohS6J5LTDHP2Rew-OxSw8qN5FiplVk/edit#gid=653576103
    # 'https://docs.google.com/spreadsheets/d/1MuEB6erMb8mD--HQLh85NDIcxL-coeoQ_wfB8ilyfak/edit#gid=1713188461'

    urls = [
        "https://docs.google.com/spreadsheets/d/1L17AVvNANbHYyLFKXlYRBEol8nZ14pvO3_KSGlQf0WE/edit#gid=1241019472",
        "https://docs.google.com/spreadsheets/d/14tBa8mqCAXw50qcQfZsrTs70LeKIPEe9yISsXxYgQUk/edit",
        "https://docs.google.com/spreadsheets/d/1qIfm2xhAIRb6kN6P1MgHMTNhlBYpLTApoMSyNa2fxk0/edit#gid=704433363",
        "https://docs.google.com/spreadsheets/d/1Mbe1_ANXUMps_LONbn8ntaARg5JqU7v1dMU8KUpnCwo/edit#gid=408034383",
        "https://docs.google.com/spreadsheets/d/16vY2NdX8IVHbeg7cHW2gSKsVd8CJiSadN33QKTz0cII/edit#gid=1243013907"
    ]
    sheet_info = sheet_type.ALBUM_IMAGE
    for url in urls:
        gsheet_id = get_gsheet_id_from_url(url=url)
        gsheet_name = get_gsheet_name(gsheet_id=gsheet_id)
        list_of_sheet_title = get_list_of_sheet_title(gsheet_id=gsheet_id)
        check_box()
        # crawl_image(sheet_info)

    # Start tools:
    check_box()
    # final_check()
    # process_mp3_mp4(sheet_info)
    # process_version_sheet(sheet_info)
    # crawl_image(sheet_info)
    # update_wiki(sheet_info)

    print("--- %s seconds ---" % (time.time() - start_time))
