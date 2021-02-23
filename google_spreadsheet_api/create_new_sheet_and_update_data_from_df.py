# https://developers.google.com/sheets/api/quickstart/python

from google_spreadsheet_api.function import add_sheet, update_value, get_list_of_sheet_title,delete_sheet

import pandas as pd


def creat_new_sheet_and_update_data_from_df(df: object, gsheet_id: str, new_sheet_name: str):
    '''
    :param df: dataframe column_type: not date_time and fillna before update value to gsheet, Eg: df.fillna(value='None').astype({"created_at": 'str'})
    :param gsheet_id:
    :param new_sheet_name:
    :return:
    '''

    list_of_sheet_title = get_list_of_sheet_title(gsheet_id)
    if new_sheet_name in list_of_sheet_title:
        delete_sheet(gsheet_id, new_sheet_name)

        column_name = df.columns.values.tolist()
        list_result = df.values.tolist()  # transfer data_frame to 2D list
        list_result.insert(0, column_name)

        add_sheet(gsheet_id, new_sheet_name)
        range_to_update = f"{new_sheet_name}!A1"
        update_value(list_result, range_to_update,
                     gsheet_id)  # validate_value type: object, int, category... NOT DATETIME
    # return print("\n complete create new sheet and update data")

    else:
        column_name = df.columns.values.tolist()
        list_result = df.values.tolist()  # transfer data_frame to 2D list
        list_result.insert(0, column_name)

        add_sheet(gsheet_id, new_sheet_name)
        range_to_update = f"{new_sheet_name}!A1"
        update_value(list_result, range_to_update,
                     gsheet_id)  # validate_value type: object, int, category... NOT DATETIME
    # return print("\n complete create new sheet and update data")



