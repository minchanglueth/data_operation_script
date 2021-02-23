# https://developers.google.com/sheets/api/quickstart/python

from core.crud.get_df_from_query import get_df_from_query
from core.crud.sql.external_identity import get_artists_from_album_ituneid
from google_spreadsheet_api.function import get_df_from_speadsheet, create_new_gsheet, \
    creat_new_sheet_and_update_data_from_df, update_value
import pandas as pd
import time


def export_artist_contribution():
    pd.set_option("display.max_rows", None, "display.max_columns", 60, 'display.width', 1000)
    df = get_df_from_speadsheet('1wMLmbaY3ZJiPDU2uekzfVVLjwHOzGJ2TpYFNCWTNqAE', 'Youtube collect_experiment')
    album_ituneid = df[(df.Itunes_ID != '')
                       & (df.Itunes_ID != 'Itunes_ID')
                       & (df.Itunes_ID.notnull())
                       ]['Itunes_ID'].drop_duplicates(keep='first').tolist()
    result = get_df_from_query(get_artists_from_album_ituneid(album_ituneid))[["name", "external_id"]]

    # Delete_data_cloumn_EF
    df2 = get_df_from_speadsheet('1zamfPcMKhk0tDjqAStA3cfS4BYiNAIZicoQJHnERRd0', 'Accumulated User Contribution')
    df2["name"] = ""
    df2["external_id"] = ""
    df_update_value = df2[["name", "external_id"]].values.tolist()
    update_value(df_update_value, "Accumulated User Contribution!E2", "1zamfPcMKhk0tDjqAStA3cfS4BYiNAIZicoQJHnERRd0")

    # Update data to gsheet_id
    list_result = result.values.tolist()
    update_value(list_result=list_result, range_to_update="Accumulated User Contribution!E2",
                 gsheet_id="1zamfPcMKhk0tDjqAStA3cfS4BYiNAIZicoQJHnERRd0")


if __name__ == "__main__":
    start_time = time.time()
    pd.set_option("display.max_rows", None, "display.max_columns", 60, 'display.width', 1000)
    # INPUT HERE
    # 'https://docs.google.com/spreadsheets/d/1Ck9O771xM7VArdaYxbHTVtp4kRtHzPn57EDDId0cHJc/edit#gid=0'
    # 'https://docs.google.com/spreadsheets/d/1wMLmbaY3ZJiPDU2uekzfVVLjwHOzGJ2TpYFNCWTNqAE/edit#gid=0'

    export_artist_contribution()

    print("--- %s seconds ---" % (time.time() - start_time))
