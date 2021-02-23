from google_spreadsheet_api.function import get_df_from_speadsheet,get_gsheet_name

import pandas as pd
import time
from core import query_path


def crawl_artist_album_from_artist_ituneid():
    '''
    invert (~) operator: https://stackoverflow.com/questions/17097643/search-for-does-not-contain-on-a-dataframe-in-pandas
    :return:
    '''

    df = get_df_from_speadsheet(gsheet_id, sheet_name).drop_duplicates(subset="external_id",keep= "first")
    row_index = df[
                    (~df.external_id.str.contains(pat='NOT_FOUND', regex=True, na=False))
                    & (df.external_id.notnull())
                    & (df.external_id != '')
                    & (df.external_id != 'None')
                   ].index
    with open(query_path, "w") as f:
        for i in row_index:
            external_id = df.external_id.loc[i]
            joy_xinh = f"insert into crawlingtasks(Id, ActionId, TaskDetail, Priority) values (uuid4(), '3FFA9CB0E221416288ACFE96B5810BD2',JSON_SET(IFNULL(crawlingtasks.TaskDetail, JSON_OBJECT()), '$.artist_id','{external_id}','$.PIC', '{gsheet_name}_{sheet_name}'),999) ;\n"
            f.write(joy_xinh)


if __name__ == "__main__":
    start_time = time.time()
    pd.set_option("display.max_rows", None, "display.max_columns", 60, 'display.width', 1000)
    # INPUT HERE
    # 'https://docs.google.com/spreadsheets/d/1cjVSAVZmGBS7D8-n7t15x-_yLvMNuTfTeqca6sIVGh8/edit#gid=1005271885'

    gsheet_id = '1cjVSAVZmGBS7D8-n7t15x-_yLvMNuTfTeqca6sIVGh8'
    gsheet_name = get_gsheet_name(gsheet_id=gsheet_id)
    sheet_name = 'Joy'

    crawl_artist_album_from_artist_ituneid()

    print("\n --- total time to process %s seconds ---" % (time.time() - start_time))
