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

    if "collection" in full_api and "track" in full_api:
        for result in results:
            if result.get('wrapperType') == "collection":
                album_title = result.get('collectionCensoredName')
                album_artist = result.get('artistName')
                print(f"{album_title}, {album_artist}")

    else:
        #   Step 2: check web url

        web_url = f"https://music.apple.com/{itune_region}/album/{itune_album_id}"
        web_response = requests.get(web_url)
        if web_response:
            html_content = web_response.content
            soup = BeautifulSoup(html_content, 'html.parser')
            # tag = soup.p['class']

            k = soup.find(id="page-container__first-linked-element")
            print(k)
            # print(soup.prettify())
            # print(soup.head.contents)
            # print(soup.body)
            # print(soup.title)
            # print(soup.a)

            # css_soup = soup.p
            # print(tag)

            try:
                album_title = soup.title.text
                # print(album_title)
            except AttributeError:
                print(f"Got error when calling url:{web_url}")
        else:
            print(f"Got error when calling url:{web_url}")

# <p class="song-stats-container typography-body">
# <div>25 SONGS, 1 HOUR, 40 MINUTES</div>
# </p>

'''
urls = ["https://docs.google.com/spreadsheets/d/1P1x71loZ1GlPCT45i3mqHV5kZI6ch-KhYYEcLi75o50/edit#gid=262810809"]
    for url in urls:
        gsheet_id = get_gsheet_id_from_url(url)
        s11_df = get_df_from_speadsheet(gsheet_id=gsheet_id, sheet_name="S_11")
        s11_df = s11_df.head(10)
        s11_df['checking_validate_itune'] = s11_df['Apple ID'].apply(lambda x: check_validate_itune(x))
        print(s11_df)
'''
if __name__ == "__main__":
    start_time = time.time()

    itune_album_id = "1553308273"
    # 1553308273
    # 1556175419
    get_album_title_artist(itune_album_id=itune_album_id)


    print("--- %s seconds ---" % (time.time() - start_time))