import time
import pandas as pd
from crawl_itune.functions import (
    get_max_ratio,
    check_validate_itune,
    get_itune_id_region_from_itune_url,
)
from google_spreadsheet_api.function import (
    get_df_from_speadsheet,
    get_list_of_sheet_title,
)

import json


def check_validate():
    original_df["itune_id"] = original_df["Itunes_Album_URL"].apply(
        lambda x: get_itune_id_region_from_itune_url(url=x)[0]
    )
    original_df["region"] = original_df["Itunes_Album_URL"].apply(
        lambda x: get_itune_id_region_from_itune_url(url=x)[1]
    )
    original_df["checking_validate_itune"] = original_df["itune_id"].apply(
        lambda x: check_validate_itune(x)
    )
    original_df["token_set_ratio"] = original_df.apply(
        lambda x: get_max_ratio(
            itune_album_id=x["itune_id"], input_album_title=x.AlbumTitle
        ),
        axis=1,
    )
    print(original_df)
    # check_original_df = original_df[(original_df['checking_validate_itune'] != True)]
    # return check_original_df.checking_validate_itune


if __name__ == "__main__":
    start_time = time.time()
    pd.set_option(
        "display.max_rows", None, "display.max_columns", 50, "display.width", 1000
    )
    urls = [
        "https://docs.google.com/spreadsheets/d/1W2QmYccbfeEAOEboKGSFWhv9hsXoQGPSZUhMP9Njsfw/edit#gid=1941765562",
        "https://docs.google.com/spreadsheets/d/15LL8rcVnsWjE7D4RvrIMRpH8Y9Lgyio-kcs4mE540MI/edit#gid=1308575784",
    ]
    sheet_info = sheet_type.S_11
    joy = process_S_11(urls=urls, sheet_info=sheet_info)
    k = check_validate(df=joy)

    # k = check_youtube_url_mp3(gsheet_id="1W2QmYccbfeEAOEboKGSFWhv9hsXoQGPSZUhMP9Njsfw", sheet_info=sheet_info)
    print("--- %s seconds ---" % (time.time() - start_time))
