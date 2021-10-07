from pandas.core.frame import DataFrame
from sqlalchemy.sql.functions import count
from Data_lake_process.class_definition import (
    WhenExist,
    PageType,
    SheetNames,
    merge_file,
    DataReports,
    get_key_value_from_gsheet_info,
    add_key_value_from_gsheet_info,
    get_gsheet_id_from_url,
)
from Data_lake_process.youtube_similarity import similarity
from core.models.data_source_format_master import DataSourceFormatMaster

from core.models.crawlingtask_action_master import V4CrawlingTaskActionMaster
from core.crud.sql.query_supporter import (
    get_crawlingtask_info,
    get_s11_crawlingtask_info,
    get_track_title_track_artist_by_ituneid_and_seq,
    get_youtube_crawlingtask_info,
    get_crawling_result_cy_itunes,
)
from support_function.slack_function.slack_message import (
    send_message_slack,
    cy_Itunes_plupdate,
    mp3_mp4_all,
)
from crawl_itune.functions import get_itune_id_region_from_itune_url
from google_spreadsheet_api.function import (
    get_sheet_id_from_gsheet_id_and_sheet_name,
    update_value,
    update_value_at_last_column,
    is_a_in_x,
    get_gsheet_column,
    delete_columns,
)
from google_spreadsheet_api.gspread_utility import get_worksheet, send_count_report
from colorama import Fore, Style
import time
from core.crud.get_df_from_query import get_df_from_query
import pandas as pd
import numpy as np
from datetime import date


def checking_image_youtube_accuracy(df: object, actionid: str):
    df["check"] = ""
    df["status"] = ""
    df["crawlingtask_id"] = ""
    row_index = df.index
    for i in row_index:
        if actionid == V4CrawlingTaskActionMaster.ARTIST_ALBUM_IMAGE:
            objectid = df["uuid"].loc[i]
        elif actionid == V4CrawlingTaskActionMaster.DOWNLOAD_VIDEO_YOUTUBE:
            objectid = df["track_id"].loc[i]

        url = df.url_to_add.loc[i]
        gsheet_info = df.gsheet_info.loc[i]
        gsheet_name = get_key_value_from_gsheet_info(
            gsheet_info=gsheet_info, key="gsheet_name"
        )
        sheet_name = get_key_value_from_gsheet_info(
            gsheet_info=gsheet_info, key="sheet_name"
        )
        PIC_taskdetail = f"{gsheet_name}_{sheet_name}"
        db_crawlingtask = get_crawlingtask_info(
            objectid=objectid, PIC=PIC_taskdetail, actionid=actionid
        )

        if db_crawlingtask:
            status = db_crawlingtask.status
            crawlingtask_id = db_crawlingtask.id
            if url in db_crawlingtask.url:
                check_accuracy = True
            else:
                check_accuracy = (
                    f"crawlingtask_id: {db_crawlingtask.id}: uuid and url not match"
                )
                print(check_accuracy)
        else:
            check_accuracy = f"file: {PIC_taskdetail}, uuid: {objectid} is missing"
            print(check_accuracy)
            status = "missing"
            crawlingtask_id = "missing"
        df.loc[i, "check"] = check_accuracy
        df.loc[i, "status"] = status
        df.loc[i, "crawlingtask_id"] = crawlingtask_id
    return df


def automate_checking_status(df: object, actionid: str):
    gsheet_infos = list(set(df.gsheet_info.tolist()))
    count = 0
    while True and count < 300:
        checking_accuracy_result = checking_image_youtube_accuracy(
            df=df, actionid=actionid
        )
        result = (
            checking_accuracy_result[
                (checking_accuracy_result["status"] != "complete")
                & (checking_accuracy_result["status"] != "incomplete")
            ].status.tolist()
            == []
        )
        if result == 1:
            for gsheet_info in gsheet_infos:
                gsheet_name = get_key_value_from_gsheet_info(
                    gsheet_info=gsheet_info, key="gsheet_name"
                )
                sheet_name = get_key_value_from_gsheet_info(
                    gsheet_info=gsheet_info, key="sheet_name"
                )
                print(
                    Fore.LIGHTYELLOW_EX
                    + f"File: {gsheet_name}, sheet_name: {sheet_name} has been crawled complete already"
                    + Style.RESET_ALL
                )
            break
        else:
            count += 1
            time.sleep(2)
            print(count, "-----", result)


def checking_s11_crawler_status(df: object):
    original_df = df.copy()
    original_df["itune_id"] = original_df["itune_album_url"].apply(
        lambda x: get_itune_id_region_from_itune_url(url=x)[0]
        if x not in ("None", "", "not found", "non", "nan", "Itunes_Album_Link")
        else "None"
    )
    original_df["url"] = original_df["gsheet_info"].apply(
        lambda x: get_key_value_from_gsheet_info(gsheet_info=x, key="url")
    )

    gsheet_infos = list(set(original_df.gsheet_info.tolist()))
    for gsheet_info in gsheet_infos:
        gsheet_name = get_key_value_from_gsheet_info(
            gsheet_info=gsheet_info, key="gsheet_name"
        )
        sheet_name = get_key_value_from_gsheet_info(
            gsheet_info=gsheet_info, key="sheet_name"
        )
        PIC_taskdetail = f"{gsheet_name}_{sheet_name}"
        url = get_key_value_from_gsheet_info(gsheet_info=gsheet_info, key="url")
        original_df_split = original_df[original_df["url"] == url].reset_index()
        count = 0
        while True and count < 300:
            checking_accuracy_result = get_df_from_query(
                get_s11_crawlingtask_info(pic=PIC_taskdetail)
            )
            checking_accuracy_result["itune_album_id"] = checking_accuracy_result[
                "itune_album_id"
            ].apply(lambda x: x.strip('"'))

            result = checking_accuracy_result[
                ~(
                    (checking_accuracy_result["06_status"] == "complete")
                    & (checking_accuracy_result["E5_status"] == "complete")
                )
                | (checking_accuracy_result["06_status"] == "incomplete")
                | (
                    (checking_accuracy_result["06_status"] == "complete")
                    & (checking_accuracy_result["E5_status"] == "incomplete")
                )
            ]

            checking = result.empty
            if checking == 1:
                print(
                    Fore.LIGHTYELLOW_EX
                    + f"File: {gsheet_name}, sheet_name: {sheet_name} has been crawled complete already"
                    + Style.RESET_ALL
                )
                data_merge = pd.merge(
                    original_df_split,
                    checking_accuracy_result,
                    how="left",
                    left_on="itune_id",
                    right_on="itune_album_id",
                    validate="m:1",
                ).fillna(value="None")
                print(data_merge)
                # update data to gsheet

                data_updated = data_merge[checking_accuracy_result.columns]
                update_value_at_last_column(
                    df_to_update=data_updated,
                    gsheet_id=get_gsheet_id_from_url(url=url),
                    sheet_name=sheet_name,
                )

                # update data report:
                data_report = data_merge[
                    ~(
                        (
                            (data_merge["itune_album_url"].isin(["not found", ""]))
                            & (data_merge["06_status"] == "None")
                            & (data_merge["E5_status"] == "None")
                        )
                        | (
                            (~data_merge["itune_album_url"].isin(["not found", ""]))
                            & (data_merge["06_status"] == "complete")
                            & (data_merge["E5_status"] == "complete")
                        )
                    )
                ]
                if data_report.empty:
                    print(
                        Fore.LIGHTYELLOW_EX
                        + f"Accuracy: ok\nStatus: ok"
                        + Style.RESET_ALL
                    )
                else:
                    print(
                        Fore.LIGHTYELLOW_EX
                        + f"Accuracy: not ok\nStatus: not ok"
                        + Style.RESET_ALL
                    )
                    columns_data_report = ["itune_id"] + list(
                        checking_accuracy_result.columns
                    )
                    data_report = data_report[columns_data_report]
                    print(data_report)

                break
            else:
                count += 1
                print(
                    Fore.LIGHTYELLOW_EX
                    + f"File: {gsheet_name}, sheet_name: {sheet_name} hasn't been crawled complete"
                    + Style.RESET_ALL
                )
                time.sleep(10)
                print(count, "-----", result)


def get_format_id_from_content_type(content_type: str):
    format_id_map = {
        "OFFICIAL_MUSIC_VIDEO": DataSourceFormatMaster.FORMAT_ID_MP4_FULL,
        "OFFICIAL_MUSIC_VIDEO_2": DataSourceFormatMaster.FORMAT_ID_MP4_FULL,
        "STATIC_IMAGE_VIDEO": DataSourceFormatMaster.FORMAT_ID_MP3_FULL,
        "COVER_VIDEO": DataSourceFormatMaster.FORMAT_ID_MP4_COVER,
        "LIVE_VIDEO": DataSourceFormatMaster.FORMAT_ID_MP4_LIVE,
        "REMIX_VIDEO": DataSourceFormatMaster.FORMAT_ID_MP4_REMIX,
        "LYRIC_VIDEO": DataSourceFormatMaster.FORMAT_ID_MP4_LYRIC,
    }
    if content_type not in format_id_map.keys():
        return "Unknown"
    else:
        return format_id_map[content_type]


def checking_c11_crawler_status(original_df: object, pre_valid: str = None):
    original_df["itune_id"] = original_df.apply(
        lambda x: get_itune_id_region_from_itune_url(url=x["itune_album_url"])[0]
        if x["itune_album_url"]
        not in ("None", "", "not found", "non", "nan", "Itunes_Album_Link")
        else x["itune_id"],
        axis=1,
    )
    original_df["url"] = original_df["gsheet_info"].apply(
        lambda x: get_key_value_from_gsheet_info(gsheet_info=x, key="url")
    )
    gsheet_infos = original_df.gsheet_info.unique()
    for gsheet_info in gsheet_infos:
        gsheet_name = get_key_value_from_gsheet_info(
            gsheet_info=gsheet_info, key="gsheet_name"
        )
        sheet_name = get_key_value_from_gsheet_info(
            gsheet_info=gsheet_info, key="sheet_name"
        )
        PIC_taskdetail = f"{gsheet_name}_{sheet_name}_{pre_valid}"
        url = get_key_value_from_gsheet_info(gsheet_info=gsheet_info, key="url")
        original_df_split = original_df[original_df["url"] == url].reset_index()
        count = 0
        while True and count < 300:
            checking_accuracy_result = get_df_from_query(
                get_s11_crawlingtask_info(pic=PIC_taskdetail)
            )
            checking_accuracy_result["itune_album_id"] = checking_accuracy_result[
                "itune_album_id"
            ].apply(lambda x: x.strip('"'))
            result = checking_accuracy_result[
                ~(
                    (
                        (checking_accuracy_result["06_status"] == "complete")
                        & (checking_accuracy_result["E5_status"] == "complete")
                    )
                    | (checking_accuracy_result["06_status"] == "incomplete")
                    | (
                        (checking_accuracy_result["06_status"] == "complete")
                        & (checking_accuracy_result["E5_status"] == "incomplete")
                    )
                )
            ]
            checking = result.empty
            if checking == 1:
                print(
                    Fore.LIGHTYELLOW_EX
                    + f"File: {gsheet_name}, sheet_name: {sheet_name} has been crawled complete already"
                    + Style.RESET_ALL
                )

                data_merge = pd.merge(
                    original_df_split,
                    checking_accuracy_result,
                    how="left",
                    left_on="itune_id",
                    right_on="itune_album_id",
                    validate="m:1",
                ).fillna(value="None")
                data_merge["06_id_x"] = data_merge.apply(
                    lambda x: x["06_id_y"]
                    if x["pre_valid"] == pre_valid
                    else x["06_id_x"],
                    axis=1,
                )
                data_merge["06_status_x"] = data_merge.apply(
                    lambda x: x["06_status_y"]
                    if x["pre_valid"] == pre_valid
                    else x["06_status_x"],
                    axis=1,
                )
                data_merge["e5_id"] = data_merge.apply(
                    lambda x: x["E5_id"] if x["pre_valid"] == pre_valid else x["e5_id"],
                    axis=1,
                )
                data_merge["e5_status"] = data_merge.apply(
                    lambda x: x["E5_status"]
                    if x["pre_valid"] == pre_valid
                    else x["e5_status"],
                    axis=1,
                )
                data_merge.columns = data_merge.columns.str.replace("06_id_x", "06_id")
                data_merge.columns = data_merge.columns.str.replace(
                    "06_status_x", "06_status"
                )
                data_merge = data_merge[original_df_split.columns]

                # update data report:
                data_report = data_merge[data_merge["pre_valid"] == pre_valid]

                data_report = data_report[
                    ~(
                        (
                            (data_report["itune_album_url"].isin(["not found", ""]))
                            & (data_report["06_status"] == "None")
                            & (data_report["e5_status"] == "None")
                        )
                        | (
                            (~data_report["itune_album_url"].isin(["not found", ""]))
                            & (data_report["06_status"] == "complete")
                            & (data_report["e5_status"] == "complete")
                        )
                    )
                ]
                if data_report.empty:
                    print(
                        Fore.LIGHTYELLOW_EX
                        + f"Accuracy: ok\nStatus: ok"
                        + Style.RESET_ALL
                    )
                    row_num = data_merge.index
                    for i in row_num:
                        if data_merge["pre_valid"].loc[i] == pre_valid:
                            itune_album_id = data_merge["itune_id"].loc[i]
                            seq = data_merge["track_title/track_num"].loc[i]
                            format_id = get_format_id_from_content_type(
                                content_type=data_merge["content type"].loc[i]
                            )
                            youtube_url = data_merge["contribution_link"].loc[i]
                            db_track = get_track_title_track_artist_by_ituneid_and_seq(
                                itune_album_id=itune_album_id, seq=seq
                            )
                            if db_track:
                                track_title = db_track.title
                                track_id = db_track.id
                                track_duration = db_track.duration_ms
                                track_similarity = similarity(
                                    track_title=track_title,
                                    youtube_url=youtube_url,
                                    formatid=format_id,
                                    duration=track_duration,
                                ).get("similarity")
                            else:
                                track_title = "not found"
                                track_id = "not found"
                                track_similarity = "not found"
                            data_merge.loc[i, "track_title"] = track_title
                            data_merge.loc[i, "track_id"] = track_id
                            data_merge.loc[i, "similarity"] = track_similarity
                        else:
                            pass
                    updated_columns = [
                        "06_id",
                        "06_status",
                        "e5_id",
                        "e5_status",
                        "track_title",
                        "track_id",
                        "similarity",
                    ]
                    print(data_merge[updated_columns])
                else:
                    print(
                        Fore.LIGHTYELLOW_EX
                        + f"Accuracy: not ok\nStatus: not ok"
                        + Style.RESET_ALL
                    )
                    updated_columns = ["06_id", "06_status", "e5_id", "e5_status"]
                # update data to gsheet
                data_updated = np.array(data_merge[updated_columns])
                # flatten
                data_up = [i for j in data_updated for i in j]
                sh = get_worksheet(url, sheet_name)
                sh_columns = (
                    sh.sheet_to_df(index=None).columns.str.strip().str.lower().tolist()
                )
                # check if sheet columns contain the updated columns in the right order
                if is_a_in_x(updated_columns, sh_columns):
                    first_col = get_gsheet_column(updated_columns, sh_columns, "first")
                    last_col = get_gsheet_column(updated_columns, sh_columns, "last")
                    first_cell = f"{first_col}2"
                    last_cell = f"{last_col}{data_merge.tail(1).index.item() + 2}"
                    sh.update_cells(first_cell, last_cell, vals=data_up)
                else:
                    print(
                        Fore.LIGHTYELLOW_EX
                        + f"list of columns to be updated does not match sheet columns"
                        + Style.RESET_ALL
                    )
                break
            else:
                count += 1
                print(
                    Fore.LIGHTYELLOW_EX
                    + f"File: {gsheet_name}, sheet_name: {sheet_name} hasn't been crawled complete"
                    + Style.RESET_ALL
                )
                time.sleep(10)
                print(count, "-----", result)


def result_d9(df: object, pre_valid: str):
    # print(df)
    original_df = df.copy()
    # print(original_df[['pointlogsid']])
    filter_df = original_df[original_df["pre_valid"] == pre_valid]
    pointlogsid_list = filter_df["pointlogsid"].tolist()

    original_df["url"] = original_df["gsheet_info"].apply(
        lambda x: get_key_value_from_gsheet_info(gsheet_info=x, key="url")
    )
    gsheet_infos = list(set(original_df.gsheet_info.tolist()))
    for gsheet_info in gsheet_infos:
        gsheet_name = get_key_value_from_gsheet_info(
            gsheet_info=gsheet_info, key="gsheet_name"
        )
        sheet_name = get_key_value_from_gsheet_info(
            gsheet_info=gsheet_info, key="sheet_name"
        )
        url = get_key_value_from_gsheet_info(gsheet_info=gsheet_info, key="url")
        original_df_split = original_df.reset_index()
        count = 0
        while True and count < 300:
            # print(get_crawling_result_cy_itunes(pointlogsid_list))
            checking_result_d9 = get_df_from_query(
                get_crawling_result_cy_itunes(pointlogsid_list)
            )
            result = checking_result_d9[
                ~(
                    (checking_result_d9["d9_status"] == "complete")
                    | (checking_result_d9["d9_status"] == "incomplete")
                    | (checking_result_d9["d9_status"] == "None")
                    | (checking_result_d9["d9_status"] == "local_pending")
                    | (checking_result_d9["d9_status"] == "ERROR")
                )
            ]

            checking = result.empty
            if checking == 1:
                print(
                    Fore.LIGHTYELLOW_EX
                    + f"D9 has been crawled complete already"
                    + Style.RESET_ALL
                )

                data_merge = pd.merge(
                    original_df_split,
                    checking_result_d9,
                    how="left",
                    left_on="pointlogsid",
                    right_on="pointlogsid",
                    validate="m:1",
                ).fillna(value="")

                data_merge["d9_id"] = ""
                data_merge.loc[data_merge["d9_id"] == "", "d9_id"] = data_merge[
                    "d9_id_y"
                ]
                data_merge.loc[data_merge["d9_id"] == "", "d9_id"] = data_merge[
                    "d9_id_x"
                ]
                data_merge["d9_status"] = ""
                data_merge.loc[data_merge["d9_status"] == "", "d9_status"] = data_merge[
                    "d9_status_y"
                ]
                data_merge.loc[data_merge["d9_status"] == "", "d9_status"] = data_merge[
                    "d9_status_x"
                ]

                # update data to gsheet
                updated_columns = ["d9_id", "d9_status"]
                data_updated = np.array(data_merge[updated_columns])
                # flatten data
                data_up = [i for j in data_updated for i in j]
                sh = get_worksheet(url, sheet_name)
                sh_columns = (
                    sh.sheet_to_df(index=None).columns.str.strip().str.lower().tolist()
                )
                # check if sheet columns contain the updated columns in the right order
                if is_a_in_x(updated_columns, sh_columns):
                    first_col = get_gsheet_column(updated_columns, sh_columns, "first")
                    last_col = get_gsheet_column(updated_columns, sh_columns, "last")
                    first_cell = f"{first_col}2"
                    last_cell = f"{last_col}{data_merge.tail(1).index.item() + 2}"
                    sh.update_cells(first_cell, last_cell, vals=data_up)
                    send_message_slack(
                        "missing songs found from itunes",
                        len(
                            data_merge[
                                (data_merge["d9_status"] == "complete")
                                & (data_merge["pre_valid"] == pre_valid)
                            ]
                        ),
                        cy_Itunes_plupdate,
                        pre_valid,
                    ).send_to_slack()
                else:
                    print(
                        Fore.LIGHTYELLOW_EX
                        + f"list of columns to be updated does not match sheet columns"
                        + Style.RESET_ALL
                    )
                break
            else:
                count += 1
                print(
                    Fore.LIGHTYELLOW_EX
                    + f"File: {gsheet_name}, sheet_name: {sheet_name} hasn't been crawled complete"
                    + Style.RESET_ALL
                )
                time.sleep(10)
                print(count, "-----", result)


def checking_youtube_crawler_status(df: object, format_id: str):
    df["check"] = ""
    df["status"] = ""
    df["crawlingtask_id"] = ""
    gsheet_infos = list(set(df.gsheet_info.tolist()))
    empty_df = pd.DataFrame(
        columns=["index", "check", "status", "crawlingtask_id", "gsheet_info"]
    )
    for gsheet_info in gsheet_infos:
        df_ = df[df.gsheet_info == gsheet_info]
        df_["trackid_url"] = (
            df_["track_id"] + df_["url_to_add"]
        )  # để merge dựa vào cả điều kiện trackid và url
        gsheet_name = get_key_value_from_gsheet_info(
            gsheet_info=gsheet_info, key="gsheet_name"
        )
        sheet_name = get_key_value_from_gsheet_info(
            gsheet_info=gsheet_info, key="sheet_name"
        )
        PIC_taskdetail = f"{gsheet_name}_{sheet_name}"
        track_id_list = df_[df_.track_id.notnull()]["track_id"].values.tolist()
        db_crawlingtask = get_df_from_query(
            get_youtube_crawlingtask_info(
                track_id=track_id_list, PIC=PIC_taskdetail, format_id=format_id
            )
        )
        db_crawlingtask.columns = [
            "crawlingtask_id",
            "objectid",
            "db_url",
            "when_exists",
            "status",
            "priority",
            "created_at",
        ]
        db_crawlingtask = db_crawlingtask.sort_values(
            "created_at", ascending=False
        ).drop_duplicates(subset="objectid")
        db_crawlingtask.db_url = db_crawlingtask.db_url.str.replace('"', "")
        db_crawlingtask["trackid_dburl"] = (
            db_crawlingtask["objectid"] + db_crawlingtask["db_url"]
        )  # để merge dựa vào cả điều kiện trackid và url
        dff = pd.merge(
            df_,
            db_crawlingtask,
            how="outer",
            left_on="trackid_url",
            right_on="trackid_dburl",
        )  # thêm outer để tìm những row nào chưa được insert vào crawlingtasks
        dff = dff[
            ~dff["gsheet_info"].isin([None, np.nan])
        ]  # loại các TH trong bảng crawlingtasks có những TH ko khớp với spreadsheet dẫn đến ko có spreadsheetid ở dff

        def check(row):
            if row["crawlingtask_id_y"] not in (None, np.nan):
                return True
            else:
                return "missing"

        def check_priority(row):
            if row["priority"] == 10000:
                if row["status_y"] not in ("complete", "incomplete"):
                    return "pending"
                else:
                    return row["status_y"]
            else:
                return row["status_y"]

        dff["status_y"] = dff.apply(check_priority, axis=1)
        dff["check"] = dff.apply(check, axis=1)
        dff = dff.fillna(value={"crawlingtask_id_y": "missing", "status_y": "missing"})
        columns_to_get = [
            "index",
            "check",
            "status_y",
            "crawlingtask_id_y",
            "gsheet_info",
        ]
        dff = dff[columns_to_get]
        dff.columns = ["index", "check", "status", "crawlingtask_id", "gsheet_info"]
        empty_df = empty_df.append(dff, ignore_index=True)

    return empty_df


def automate_checking_youtube_crawler_status(
    original_df: object, filter_df: object, format_id: str
):

    checking_accuracy_result = checking_youtube_crawler_status(
        df=filter_df, format_id=format_id
    )
    gsheet_infos = list(set(checking_accuracy_result.gsheet_info.tolist()))
    for gsheet_info in gsheet_infos:
        gsheet_name = get_key_value_from_gsheet_info(
            gsheet_info=gsheet_info, key="gsheet_name"
        )
        sheet_name = get_key_value_from_gsheet_info(
            gsheet_info=gsheet_info, key="sheet_name"
        )
        url = get_key_value_from_gsheet_info(gsheet_info=gsheet_info, key="url")
        checking_accuracy_result_ = checking_accuracy_result[
            checking_accuracy_result.gsheet_info == gsheet_info
        ].copy()
        result = checking_accuracy_result_[
            ~checking_accuracy_result_["status"].isin(
                ["complete", "incomplete", "missing", "pending"]
            )
        ].status
        if len(result) == 0:
            original_df_ = original_df[original_df.gsheet_info == gsheet_info].copy()
            print(
                Fore.LIGHTYELLOW_EX
                + f"File: {gsheet_name}, sheet_name: {sheet_name}, inserted crawlingtask(s) have been finished crawling already"
                + Style.RESET_ALL
            )
            missing_crawlingtasks = len(
                checking_accuracy_result_[
                    checking_accuracy_result_["status"] == "missing"
                ]
            )
            print(
                Fore.LIGHTRED_EX
                + f"There are {missing_crawlingtasks} row(s) have NOT been inserted to crawlingtasks. Please check row(s) with 'missing' crawlingtask_id"
                + Style.RESET_ALL
            )
            updated_column = ["check", "status", "crawlingtask_id"]
            merge_df = original_df_.merge(
                checking_accuracy_result_[
                    ["check", "status", "crawlingtask_id", "index"]
                ],
                left_index=True,
                right_on="index",
                how="left",
            ).fillna(value="")
            columns_todelete = ["check", "status", "crawlingtask_id"]
            delete_columns(
                sheet_name=sheet_name,
                gsheet_id=get_gsheet_id_from_url(url=url),
                colNames=columns_todelete,
            )
            update_value_at_last_column(
                df_to_update=merge_df[updated_column],
                gsheet_id=get_gsheet_id_from_url(url=url),
                sheet_name=sheet_name,
            )
            count_crawlingtaskid = len(
                merge_df[
                    ~merge_df["crawlingtask_id"].isin([None, "", np.nan, "missing"])
                ]
            )
            report_data = [
                str(date.today()),
                gsheet_name,
                url,
                sheet_name,
                count_crawlingtaskid,
            ]
            for status in ["complete", "incomplete", "pending", "missing"]:
                count_status = len(merge_df[merge_df["status"] == status])
                report_data.append(count_status)
            res = send_count_report(
                sheet_name="artist_page",
                number_cols=len(report_data),
                data_to_insert=report_data,
            )
            if res == False:
                message = mp3_mp4_all.format(*report_data[1:])
                send_message_slack(
                    "none", "none", "none", "none", message
                ).send_to_slack_mp3mp4()

        else:
            print(
                Fore.LIGHTYELLOW_EX
                + f"File: {gsheet_name}, sheet_name: {sheet_name} hasn't been crawled completely"
                + Style.RESET_ALL
            )
            time.sleep(10)
            print(result)


# if __name__ == "__main__":
#     start_time = time.time()

#     pd.set_option(
#         "display.max_rows", None, "display.max_columns", 30, "display.width", 500
#     )
