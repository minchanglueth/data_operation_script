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
        gsheet_file.sheet_to_df(index=None)
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


def get_gsheet_column(columns_to_update: list, worksheet_columns: list, position: str):
    """
    Get the corresponding gsheet column (A, B, C) from dataframe column name
    """
    if position == "first":
        column = columns_to_update[0]
    elif position == "last":
        column = columns_to_update[-1]

    column_index = worksheet_columns.index(column)
    if column_index <= 25:
        return string.ascii_uppercase[(column_index + 1)]
    elif column_index <= 51:
        return f"A{string.ascii_uppercase[(column_index - 25)]}"
    else:
        return f"B{string.ascii_uppercase[(column_index - 51)]}"


def send_count_report(sheet_name: str, number_cols: int, data_to_insert):
    gsheet_url = "https://docs.google.com/spreadsheets/d/1MHDksbs-RKXhZZ-LRgRhVy_ldAxK8lSzyoJK4sA_Uyo/edit#gid=209567714"
    sheet = gc.open_by_url(gsheet_url)
    sh = sheet.worksheet(sheet_name)
    sheet_df = get_as_dataframe(sh, usecols=list(range(number_cols))).dropna(how="all")
    add_df = pd.DataFrame([data_to_insert], columns=sheet_df.columns)
    res = (
        sheet_df[sheet_df.columns[1:]]
        .isin(data_to_insert[1:])
        .all(axis="columns")
        .any()
    )  # [1:] là để loại cột Date/created_at khi so sánh sheet_df có chứa dòng data_to_insert ko
    if res == False:
        set_with_dataframe(
            sh, add_df, row=len(sheet_df) + 2, include_column_header=False
        )
    return res


# if __name__ == "__main__":
#     print(string.ascii_uppercase[25])
