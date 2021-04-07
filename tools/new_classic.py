import time
from typing import List
import logging
import requests
import json
import logging
import pandas as pd
from bs4 import BeautifulSoup
from tools.data_lake_standard import get_gsheet_id_from_url
from google_spreadsheet_api.function import get_df_from_speadsheet, get_list_of_sheet_title, update_value, \
    creat_new_sheet_and_update_data_from_df, get_gsheet_name
import random
from support_function.text_similarity.text_similarity import get_token_set_ratio


def get_itunes_api_result(url: str) -> List[dict]:
    try:
        for _ in range(0, 2):
            response = requests.get(url)
            if response:
                response_map = response.json()
                results = response_map.get("results")
                sleep_time = random.uniform(0.5, 1)
                time.sleep(sleep_time)
                return results
    except Exception:
        logging.debug(f"Got error when calling url [{url}]")
    return []


def get_track_info_by_seq(url: str, seq: int):
    results = get_itunes_api_result(url)
    track_info = {}
    seq = -1
    for result in results:
        seq = seq + 1
        result.update({"seq": seq})
        if result.get("kind") == "song" and result.get("seq") == seq:
            result.update({"Check track existed": True})
            track_info = result

    if not track_info:
        track_info = {"Check track existed": False}  # a jay ko day cho nay`
    print(track_info)
    return track_info


def check_validate_itune(itune_album_id: str, itune_region: str = "us"):
    #     Step 1: check api
    api_url = f"http://itunes.apple.com/lookup?id={itune_album_id}&entity=song&country={itune_region}&limit=1000"
    results = get_itunes_api_result(api_url)
    full_api = []
    for result in results:
        wrapperType = result.get('wrapperType')
        full_api.append(wrapperType)
        full_api = list(set(full_api))
    if "collection" in full_api and "track" in full_api:
        return True
    else:
        #   Step 2: check web url

        web_url = f"https://music.apple.com/{itune_region}/album/{itune_album_id}"
        web_response = requests.get(web_url)
        if web_response:
            html_content = web_response.content
            soup = BeautifulSoup(html_content, 'html.parser')
            try:
                # check existed albumname = soup.title.text
                soup.title.text
                return True
            except AttributeError:
                return False
        else:
            return False


def get_album_title_artist(itune_album_id: str, itune_region: str = "us"):
    #     Step 1: check api
    api_url = f"http://itunes.apple.com/lookup?id={itune_album_id}&entity=song&country={itune_region}&limit=1000"
    results = get_itunes_api_result(api_url)
    full_api = []
    for result in results:
        wrapperType = result.get('wrapperType')
        full_api.append(wrapperType)
        full_api = list(set(full_api))
    album_info = []
    if "collection" in full_api and "track" in full_api:
        for result in results:
            if result.get('wrapperType') == "collection":
                album_title = result.get('collectionCensoredName')
                album_artist = result.get('artistName')
                album_info.append(album_title)
                album_info.append(album_artist)

    else:
        #   Step 2: check web url
        web_url = f"https://music.apple.com/{itune_region}/album/{itune_album_id}"
        web_response = requests.get(web_url)
        if web_response:
            html_content = web_response.content
            soup = BeautifulSoup(html_content, 'html.parser')
            try:
                album_title_tag = soup.find_all(id="page-container__first-linked-element")
                album_title = album_title_tag[0].text.strip()

                artist_album_tag = soup.find_all("div", {"class": "product-creator typography-large-title"})
                artist_album = artist_album_tag[0].text.strip()
                album_info.append(album_title)
                album_info.append(artist_album)
            except AttributeError:
                print(f"Got error when calling url:{web_url}")
            except IndexError:
                print(f"Got error when calling url:{web_url}")
            except:
                print(f"Got error when calling url:{web_url}")
        else:
            print(f"Got error when calling url:{web_url}")
    # print(f"{album_info[0]}, {album_info[1]}")
    return album_info


def get_tracklist_from_album_itune(itune_album_id: str, itune_region: str = "us"):
    #     Step 1: check api
    api_url = f"http://itunes.apple.com/lookup?id={itune_album_id}&entity=song&country={itune_region}&limit=1000"
    results = get_itunes_api_result(api_url)
    full_api = []
    for result in results:
        wrapperType = result.get('wrapperType')
        full_api.append(wrapperType)
        full_api = list(set(full_api))
    album_info = []
    track_info = []
    if "collection" in full_api and "track" in full_api:
        for result in results:
            if result.get('wrapperType') == "collection":
                album_title = result.get('collectionCensoredName')
                album_artist = result.get('artistName')
                album_info.append(album_title)
                album_info.append(album_artist)
            else:
                track_title = result.get('trackCensoredName')
                track_artist = result.get('artistName')
                track_2D = [track_title, track_artist]
                track_info.append(track_2D)



    else:
        #   Step 2: check web url
        web_url = f"https://music.apple.com/{itune_region}/album/{itune_album_id}"
        print(web_url)
        web_response = requests.get(web_url)
        if web_response:
            html_content = web_response.content
            soup = BeautifulSoup(html_content, 'html.parser')
            try:
                album_title_tag = soup.find_all(id="page-container__first-linked-element")
                album_title = album_title_tag[0].text.strip()

                artist_album_tag = soup.find_all("div", {"class": "product-creator typography-large-title"})
                artist_album = artist_album_tag[0].text.strip()
                album_info.append(album_title)
                album_info.append(artist_album)

                song_names = soup.find_all("div", {"class": "song-name typography-body-tall"})
                for song_name in song_names:
                    song_name = song_name.text.strip()
                    track_2D = [song_name, artist_album]
                    track_info.append(track_2D)
            except AttributeError:
                print(f"Got error when calling url:{web_url}")
            except IndexError:
                print(f"Got error when calling url:{web_url}")
            except:
                print(f"Got error when calling url:{web_url}")
        else:
            print(f"Got error when calling url:{web_url}")
    return track_info


def get_max_ratio(itune_album_id: str, input_album_title: str):
    album_title = get_album_title_artist(itune_album_id=itune_album_id)[0]
    max_ratio = get_token_set_ratio(str1=input_album_title, str2=album_title)

    if max_ratio != 100:
        tracklist = list(get_tracklist_from_album_itune(itune_album_id=itune_album_id))
        for song in tracklist:
            song = song[0]
            k = get_token_set_ratio(str1=song, str2=input_album_title)
            if max_ratio >= k:
                max_ratio = max_ratio
            else:
                max_ratio = k
    return max_ratio


if __name__ == "__main__":
    start_time = time.time()
    pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)

    # itune_album_id = "1560734944"
    # get_max_ratio(itune_album_id=itune_album_id, input_album_title="deja vu")
    # 1553308273
    # 1556175419
    urls = [
        "https://docs.google.com/spreadsheets/d/1W2QmYccbfeEAOEboKGSFWhv9hsXoQGPSZUhMP9Njsfw/edit#gid=1941765562"
    ]

    for url in urls:
        gsheet_id = get_gsheet_id_from_url(url)
        s11_df = get_df_from_speadsheet(gsheet_id=gsheet_id, sheet_name="S_11")
        s11_df = s11_df[(s11_df['Itunes_Album_URL'] != "not found")]
        # s11_df = s11_df.head(10)
        s11_df['checking_validate_itune'] = s11_df['Apple ID'].apply(lambda x: check_validate_itune(x))
        s11_df['album_title'] = s11_df['Apple ID'].apply(lambda x: get_album_title_artist(x)[0])
        # s11_df['artist_album'] = s11_df['Apple ID'].apply(lambda x: get_album_title_artist(x)[1])
        s11_df['token_set_ratio'] = s11_df.apply(
            lambda x: get_max_ratio(itune_album_id=x['Apple ID'], input_album_title=x.AlbumTitle), axis=1)
        print(s11_df)

    print("--- %s seconds ---" % (time.time() - start_time))
