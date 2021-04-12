import time
from typing import List
import requests
import logging
import pandas as pd
from bs4 import BeautifulSoup
import random
from support_function.text_similarity.text_similarity import get_token_set_ratio


def get_itunes_api_result(url: str) -> List[dict]:
    try:
        for _ in range(0, 2):
            response = requests.get(url)
            if response:
                response_map = response.json()
                results = response_map.get("results")
                time.sleep(2)
                return results
            time.sleep(2)
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
    if not album_info:
        album_info = ["Itunes returns no result through look up api", "Itunes returns no result through look up api"]
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


def get_itune_id_region_from_itune_url(url: str):
    itune_id = url.split("/")[-1]
    itune_region = url.split("/")[3]
    return [itune_id, itune_region]


if __name__ == "__main__":
    start_time = time.time()
    pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)
    itune_url = "https://music.apple.com/us/album/montero-call-me-by-your-name-single/joytest"
    # https://music.apple.com/us/album/fearless-taylors-version/1552791073
    # https://music.apple.com/us/album/montero-call-me-by-your-name-single/joytest
    joy = get_itune_id_region_from_itune_url(url=itune_url)
    k = get_album_title_artist(itune_album_id=joy[0], itune_region=joy[1])
    # get_max_ratio(itune_album_id=joy)
    print(k)

    print("--- %s seconds ---" % (time.time() - start_time))
