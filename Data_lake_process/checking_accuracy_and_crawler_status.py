from Data_lake_process.class_definition import WhenExist, PageType, SheetNames, merge_file, DataReports, \
    get_key_value_from_gsheet_info, add_key_value_from_gsheet_info, get_gsheet_id_from_url

from core.models.crawlingtask_action_master import V4CrawlingTaskActionMaster
from core.crud.sql.query_supporter import get_crawlingtask_info, get_s11_crawlingtask_info
from colorama import Fore, Style
import time
from core.crud.get_df_from_query import get_df_from_query
import pandas as pd


def checking_image_youtube_accuracy(df: object, actionid: str):
    df['check'] = ''
    df['status'] = ''
    df['crawlingtask_id'] = ''
    row_index = df.index
    for i in row_index:
        if actionid == V4CrawlingTaskActionMaster.ARTIST_ALBUM_IMAGE:
            objectid = df['uuid'].loc[i]
        elif actionid == V4CrawlingTaskActionMaster.DOWNLOAD_VIDEO_YOUTUBE:
            objectid = df['track_id'].loc[i]

        url = df.url_to_add.loc[i]
        gsheet_info = df.gsheet_info.loc[i]
        gsheet_name = get_key_value_from_gsheet_info(gsheet_info=gsheet_info, key='gsheet_name')
        sheet_name = get_key_value_from_gsheet_info(gsheet_info=gsheet_info, key='sheet_name')
        PIC_taskdetail = f"{gsheet_name}_{sheet_name}"
        db_crawlingtask = get_crawlingtask_info(objectid=objectid, PIC=PIC_taskdetail, actionid=actionid)

        if db_crawlingtask:
            status = db_crawlingtask.status
            crawlingtask_id = db_crawlingtask.id
            if url in db_crawlingtask.url:
                check_accuracy = True
            else:
                check_accuracy = f"crawlingtask_id: {db_crawlingtask.id}: uuid and url not match"
                print(check_accuracy)
        else:
            check_accuracy = f"file: {PIC_taskdetail}, uuid: {objectid} is missing"
            print(check_accuracy)
            status = 'missing'
            crawlingtask_id = 'missing'
        df.loc[i, 'check'] = check_accuracy
        df.loc[i, 'status'] = status
        df.loc[i, 'crawlingtask_id'] = crawlingtask_id
    return df


def automate_checking_status(df: object, actionid: str):
    gsheet_infos = list(set(df.gsheet_info.tolist()))
    count = 0
    while True and count < 300:
        checking_accuracy_result = checking_image_youtube_accuracy(df=df, actionid=actionid)
        result = checking_accuracy_result[
                     (checking_accuracy_result['status'] != 'complete')
                     & (checking_accuracy_result['status'] != 'incomplete')
                     ].status.tolist() == []
        if result == 1:
            for gsheet_info in gsheet_infos:
                gsheet_name = get_key_value_from_gsheet_info(gsheet_info=gsheet_info, key='gsheet_name')
                sheet_name = get_key_value_from_gsheet_info(gsheet_info=gsheet_info, key='sheet_name')
                print(
                    Fore.LIGHTYELLOW_EX + f"File: {gsheet_name}, sheet_name: {sheet_name} has been crawled complete already" + Style.RESET_ALL)
            break
        else:
            count += 1
            time.sleep(2)
            print(count, "-----", result)


def checking_s11_accuracy(df: object, actionid: str):

        gsheet_info = df.gsheet_info.loc[i]
        gsheet_name = get_key_value_from_gsheet_info(gsheet_info=gsheet_info, key='gsheet_name')
        sheet_name = get_key_value_from_gsheet_info(gsheet_info=gsheet_info, key='sheet_name')
        PIC_taskdetail = f"{gsheet_name}_{sheet_name}"
        checking_df = get_df_from_query(query=get_s11_crawlingtask_info(pic=PIC_taskdetail))
        print(checking_df)

if __name__ == "__main__":
    start_time = time.time()

    pd.set_option("display.max_rows", None, "display.max_columns", 30, 'display.width', 500)