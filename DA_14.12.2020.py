import pandas as pd
from google_spreadsheet_api.function import get_df_from_speadsheet

if __name__ == "__main__":
    # pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)
    # 'https://docs.google.com/spreadsheets/d/1V0t7sOeIA4MJsLOgr98kV_y9A56IVUk9qhfE7LeaB5M/edit#gid=784408105'
    # gsheetid= '1V0t7sOeIA4MJsLOgr98kV_y9A56IVUk9qhfE7LeaB5M'
    # sheet_name = 'Sheet8'
    # df = get_df_from_speadsheet(gsheet_id=gsheetid, sheet_name=sheet_name)
    # print(df.head(10))

    joy = "Songbird (Live from Kilo Kilo Studio 2018)"
    k = joy.replace("Songbird", "").strip()[1:-1]
    raw_year = k.split(' ')[-1]
    if raw_year.isnumeric():
        year = raw_year
        concert_live_name = k.replace(year, "")

    print(year)
    print(concert_live_name)
