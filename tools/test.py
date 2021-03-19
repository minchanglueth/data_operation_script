import pandas as pd
import numpy as np
from google_spreadsheet_api.function import creat_new_sheet_and_update_data_from_df

if __name__ == "__main__":
    # gsheet_id: 1mBJcQvqobNfISSrandRrBCpQCY8vocDi7w6HPUXOY-A, gsheet_name: Top 100 Albums 08.03.2021, sheet_name: artist image cant upload
    d = {
        "one": pd.Series([1.0, 2.0, 3.0, 5], index=["a", "b", "c", "d"]),
         "two": pd.Series([1.0, 2.0, 3.0, 4.0], index=["a", "b", "c", "d"])
                                  }
    df = pd.DataFrame(d)
    print(df)

    new_sheet_name = 'artist image cant upload'
    creat_new_sheet_and_update_data_from_df(df=df, gsheet_id="1mBJcQvqobNfISSrandRrBCpQCY8vocDi7w6HPUXOY-A",
                                            new_sheet_name=new_sheet_name)
