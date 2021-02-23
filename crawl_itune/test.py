import time
from typing import List
import logging
import requests
import json
import logging
# from scrapy import Selector
import scrapy


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


if __name__ == "__main__":
    url = "http://itunes.apple.com/lookup?id=1444054130&entity=song&country=us&limit=1000"
    seq = 5
    data = get_track_info_by_seq(url=url, seq=seq)

    # print(k[1].get(k, default = None))

    # url = "https://music.apple.com/us/album/clube-da-esquina-2/1361080094?uo=4"

    # get_itunes_api_result()
