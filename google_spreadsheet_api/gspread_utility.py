from google_spreadsheet_api.function import get_list_of_sheet_title
import gspread
from gspread_pandas import Spread, Client
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
import numpy as np
import string


# gspread.auth.DEFAULT_CREDENTIALS_FILENAME = credentials_path

gc = gspread.service_account()

# config_dir = os.path.join(BASE_DIR, "sources")

# config = conf.get_config(conf_dir=config_dir, file_name="credentials.json")


def get_worksheet(url, sheet_name):
    return Spread(spread=url, sheet=sheet_name)


def get_df_from_gsheet(url, sheet_name):
    gsheet_file = Spread(spread=url, sheet=sheet_name)
    df = (
        gsheet_file.sheet_to_df()
        .apply(lambda x: x.str.strip())
        .fillna(value="")
        .astype(str)
    )
    return df


def get_list_of_sheet_titles(gsheet_url: str):
    sh = gc.open_by_url(gsheet_url)
    sheet_data = sh.fetch_sheet_metadata()
    return [x["properties"]["title"] for x in sheet_data["sheets"]]


def create_new_sheet_and_update_date(df: object, gsheet_url: str, new_sheet_name: str):
    sh = gc.open_by_url(gsheet_url)
    title_list = get_list_of_sheet_title(gsheet_url)
    if new_sheet_name in title_list:
        sh.del_worksheet(sh.worksheet(new_sheet_name))
    pass


def check_column_name(sheet: object, column_name: list):
    # sheet_columns =
    pass


if __name__ == "__main__":
    print(string.ascii_uppercase[5])
