from sqlalchemy.sql.functions import current_date
import gspread
import string
from google_spreadsheet_api.function import (
    get_df_from_speadsheet,
    creat_new_sheet_and_update_data_from_df,
    get_gsheet_name,
)
from core.models.crawlingtask_action_master import V4CrawlingTaskActionMaster
from core.models.data_source_format_master import DataSourceFormatMaster
from core.crud.sql import artist, album
from core.crud.sql.track import get_one_track_by_id
import pandas as pd
import numpy as np
import time
from inspect import signature
from core import query_path
from colorama import Fore, Style
from Data_lake_process.crawlingtask import (
    crawl_image,
    crawl_youtube_mp3,
    crawl_youtube_mp4,
    crawl_itunes_album,
    update_contribution,
)
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
from Data_lake_process.new_check_box_standard import (
    youtube_check_box,
    s11_checkbox,
    update_s11_check_box,
    c11_checkbox,
    update_c11_check_box,
)
from Data_lake_process.data_report import update_data_reports
from Data_lake_process.checking_accuracy_and_crawler_status import (
    checking_image_youtube_accuracy,
    automate_checking_status,
    checking_s11_crawler_status,
    checking_c11_crawler_status,
    checking_youtube_crawler_status,
    automate_checking_youtube_crawler_status,
    result_d9,
)
from crawl_itune.functions import get_itune_id_region_from_itune_url
from core.crud.get_df_from_query import get_df_from_query
from core.crud.sql.query_supporter import (
    get_pointlogsid_valid,
    get_pointlogsid_valid_for_contribution,
)
from google_spreadsheet_api.function import update_value, update_value_at_last_column
from Data_lake_process.class_definition import get_gsheet_id_from_url
from datetime import date
from Data_lake_process.youtube_similarity import similarity
from google_spreadsheet_api.gspread_utility import get_worksheet, gc
from gspread_dataframe import set_with_dataframe, get_as_dataframe

# from support_function.slack_function.slack_message import (
#     send_message_slack,
#     cy_Itunes_plupdate,
# )

# gc = gspread.oauth()


def upload_image_cant_crawl(checking_accuracy_result: object, sheet_name: str):
    gsheet_infos = list(set(checking_accuracy_result.gsheet_info.tolist()))
    df_incomplete = (
        checking_accuracy_result[(checking_accuracy_result["status"] == "incomplete")]
        .reset_index()
        .copy()
    )

    df_incomplete["url"] = df_incomplete["gsheet_info"].apply(
        lambda x: get_key_value_from_gsheet_info(gsheet_info=x, key="url")
    )
    df_incomplete["url_to_add"] = ""
    if sheet_name == SheetNames.ARTIST_IMAGE:
        df_incomplete["name"] = df_incomplete["uuid"].apply(
            lambda x: artist.get_one_by_id(artist_uuid=x).name
        )
        df_incomplete = df_incomplete[
            ["uuid", "name", "status", "crawlingtask_id", "url", "memo", "url_to_add"]
        ]
    else:
        df_incomplete["title"] = df_incomplete["uuid"].apply(
            lambda x: album.get_one_by_id(album_uuid=x).title
        )
        df_incomplete["artist"] = df_incomplete["uuid"].apply(
            lambda x: album.get_one_by_id(album_uuid=x).artist
        )
        df_incomplete["itunes_url"] = df_incomplete["uuid"].apply(
            lambda x: album.get_itunes_url(album_uuid=x).itunes_url
        )
        df_incomplete = df_incomplete[
            [
                "uuid",
                "title",
                "artist",
                "itunes_url",
                "status",
                "crawlingtask_id",
                "url",
                "memo",
                "url_to_add",
            ]
        ]

    for gsheet_info in gsheet_infos:
        url = get_key_value_from_gsheet_info(gsheet_info=gsheet_info, key="url")
        df_incomplete_to_upload = df_incomplete[
            df_incomplete["url"] == url
        ].reset_index()
        count_incomplete = df_incomplete_to_upload.index.stop
        joy = df_incomplete_to_upload["status"].tolist() == []

        if joy:
            raw_df_to_upload = {"status": ["Upload thành công 100% nhé các em ^ - ^"]}
            df_to_upload = pd.DataFrame(data=raw_df_to_upload)
        else:
            df_to_upload = df_incomplete_to_upload.drop(["url", "index"], axis=1)
        new_sheet_name = f"{sheet_name_} cant upload"
        print(df_to_upload)
        creat_new_sheet_and_update_data_from_df(
            df_to_upload, get_gsheet_id_from_url(url), new_sheet_name
        )


def query_pandas_to_csv(df: object, column: str):
    row_index = df.index
    with open(query_path, "w") as f:
        for i in row_index:
            line = df[column].loc[i]
            f.write(line)
    f.close()


def send_count_report(sheet_name: str, number_cols: int, data_to_insert):
    gsheet_url = "https://docs.google.com/spreadsheets/d/1MHDksbs-RKXhZZ-LRgRhVy_ldAxK8lSzyoJK4sA_Uyo/edit#gid=209567714"
    sheet = gc.open_by_url(gsheet_url)
    sh = sheet.worksheet(sheet_name)
    sheet_df = get_as_dataframe(sh, usecols=list(range(number_cols))).dropna(how="all")
    df = pd.DataFrame([data_to_insert], columns=sheet_df.columns)
    sheet_df = pd.concat([sheet_df, df])
    set_with_dataframe(sh, sheet_df)


class ImageWorking:
    def __init__(self, sheet_name: str, urls: list, page_type: object):
        original_file_ = merge_file(
            sheet_name=sheet_name, urls=urls, page_type=page_type
        )
        if original_file_.empty:
            print("original_file is empty")
            pass
        else:
            self.original_file = original_file_
            self.sheet_name = sheet_name
            self.page_type = page_type

    def check_box(self):
        print(self.original_file.head(10))

    def image_filter(self):
        df = self.original_file
        filter_df = (
            df[
                (
                    (df["memo"] == "missing") | (df["memo"] == "added")
                )  # filter df by conditions
                & (df["url_to_add"].notnull())
                & (df["url_to_add"] != "")
            ]
            .drop_duplicates(subset=["uuid", "url_to_add", "gsheet_info"], keep="first")
            .reset_index()
        )

        if self.sheet_name == SheetNames.ARTIST_IMAGE:
            object_type_ = {"object_type": "artist"}
            filter_df["gsheet_info"] = filter_df.apply(
                lambda x: add_key_value_from_gsheet_info(
                    gsheet_info=x["gsheet_info"], key_value=object_type_
                ),
                axis=1,
            )

        elif self.sheet_name == SheetNames.ALBUM_IMAGE:
            object_type_ = {"object_type": "album"}
            filter_df["gsheet_info"] = filter_df["gsheet_info"].apply(
                lambda x: add_key_value_from_gsheet_info(
                    gsheet_info=x, key_value=object_type_
                )
            )
        else:
            pass
        return filter_df

    def crawl_image_datalake(self, when_exists: str = WhenExist.REPLACE):
        df = self.image_filter()
        if df.empty:
            print(Fore.LIGHTYELLOW_EX + f"Image file is empty" + Style.RESET_ALL)
        else:
            df["query"] = df.apply(
                lambda x: crawl_image(
                    object_type=get_key_value_from_gsheet_info(
                        gsheet_info=x["gsheet_info"], key="object_type"
                    ),
                    url=x["url_to_add"],
                    objectid=x["uuid"],
                    when_exists=when_exists,
                    pic=f"{get_key_value_from_gsheet_info(gsheet_info=x['gsheet_info'], key='gsheet_name')}_{get_key_value_from_gsheet_info(gsheet_info=x['gsheet_info'], key='sheet_name')}",
                    priority=get_key_value_from_gsheet_info(
                        gsheet_info=x["gsheet_info"], key="page_priority"
                    ),
                ),
                axis=1,
            )
            query_pandas_to_csv(df=df, column="query")

    def checking_image_crawler_status(self):
        print("checking accuracy")
        df = self.image_filter().copy()
        gsheet_infos = list(set(df.gsheet_info.tolist()))
        # step 1.1: checking accuracy
        checking_accuracy_result = checking_image_youtube_accuracy(
            df=df, actionid=V4CrawlingTaskActionMaster.ARTIST_ALBUM_IMAGE
        )
        accuracy_checking = list(set(checking_accuracy_result["check"].tolist()))

        if accuracy_checking != [True]:
            print(
                checking_accuracy_result[["uuid", "check", "status", "crawlingtask_id"]]
            )
            # Step 1.2: update data_reports if checking accuracy fail
            for gsheet_info in gsheet_infos:
                update_data_reports(
                    gsheet_info=gsheet_info,
                    status=DataReports.status_type_processing,
                    notice="check accuracy fail",
                )
        # Step 2: auto checking status
        else:
            print("checking accuracy correctly, now checking status")
            automate_checking_status(
                df=df, actionid=V4CrawlingTaskActionMaster.ARTIST_ALBUM_IMAGE
            )
            # Step 3: upload image cant crawl
            upload_image_cant_crawl(
                checking_accuracy_result=checking_accuracy_result,
                sheet_name=self.sheet_name,
            )


# crawl_image_datalake = ImageWorking.crawl_image_datalake
# checking_image_crawler_status = ImageWorking.checking_image_crawler_status


class YoutubeWorking:
    def __init__(self, sheet_name: str, urls: list, page_type: object):
        original_file_ = merge_file(
            sheet_name=sheet_name, urls=urls, page_type=page_type
        )
        if original_file_.empty:
            print("original_file is empty")
            pass
        else:
            self.original_file = original_file_
            self.sheet_name = sheet_name
            self.page_type = page_type

    def check_box(self):
        df = self.original_file
        youtube_check_box(
            page_name=getattr(self.page_type, "name"), df=df, sheet_name=self.sheet_name
        )
        return youtube_check_box

    def youtube_filter(self):
        if self.check_box():
            df = self.original_file
            if self.sheet_name == SheetNames.MP3_SHEET_NAME:
                filter_df = (
                    df[
                        (
                            (df["memo"] == "not ok") | (df["memo"] == "added")
                        )  # filter df by conditions
                        & (df["url_to_add"].notnull())
                        & (df["url_to_add"] != "")
                    ]
                    .drop_duplicates(
                        subset=["track_id", "url_to_add", "type", "gsheet_info"],
                        keep="first",
                    )
                    .reset_index()
                )
            elif self.sheet_name == SheetNames.MP4_SHEET_NAME:
                filter_df = (
                    df[
                        (
                            (df["memo"] == "not ok") | (df["memo"] == "added")
                        )  # filter df by conditions
                        & (df["url_to_add"].notnull())
                        & (df["url_to_add"] != "")
                    ]
                    .drop_duplicates(
                        subset=["track_id", "url_to_add", "gsheet_info"], keep="first"
                    )
                    .reset_index()
                )
            return filter_df

    def crawl_mp3_mp4_youtube_datalake(self):
        df = self.youtube_filter()
        if self.sheet_name == SheetNames.MP3_SHEET_NAME:
            crawl_youtube_mp3(df=df)
        elif self.sheet_name == SheetNames.MP4_SHEET_NAME:
            crawl_youtube_mp4(df=df)
        else:
            pass

    def checking_youtube_crawler_status(self):
        print("checking accuracy")
        original_df = self.original_file
        filter_df = self.youtube_filter()
        if self.sheet_name == SheetNames.MP3_SHEET_NAME:
            format_id = DataSourceFormatMaster.FORMAT_ID_MP3_FULL
        elif self.sheet_name == SheetNames.MP4_SHEET_NAME:
            format_id = DataSourceFormatMaster.FORMAT_ID_MP4_FULL
        else:
            print("format_id not support")
            pass
        automate_checking_youtube_crawler_status(
            original_df=self.original_file,
            filter_df=self.youtube_filter().copy(),
            format_id=format_id,
        )

    def similarity(self):
        df = self.original_file
        df["similarity"] = ""
        df["note"] = ""
        if self.sheet_name == SheetNames.MP3_SHEET_NAME:
            format_id = DataSourceFormatMaster.FORMAT_ID_MP3_FULL
        elif self.sheet_name == SheetNames.MP4_SHEET_NAME:
            format_id = DataSourceFormatMaster.FORMAT_ID_MP4_FULL
        else:
            print("format_id not support")
            pass
        gsheet_info = list(set(df.gsheet_info.tolist()))[0]
        sheet_name = get_key_value_from_gsheet_info(
            gsheet_info=gsheet_info, key="sheet_name"
        )
        url = get_key_value_from_gsheet_info(gsheet_info=gsheet_info, key="url")
        row_num = df.index
        for i in row_num:
            if df["memo"].loc[i] == "added":
                trackid = df["track_id"].loc[i]
                youtube_url = df["url_to_add"].loc[i]
                db_track = get_one_track_by_id(track_id=trackid)
                if db_track:
                    track_title = db_track.title
                    track_duration = db_track.duration_ms
                    track_similarity = similarity(
                        track_title=track_title,
                        youtube_url=youtube_url,
                        formatid=format_id,
                        duration=track_duration,
                    ).get("similarity")
                else:
                    track_similarity = "not found"
                df.loc[i, "similarity"] = track_similarity
            else:
                pass

        update_value_at_last_column(
            df_to_update=df[["similarity", "note"]],
            gsheet_id=get_gsheet_id_from_url(url=url),
            sheet_name=sheet_name,
        )


class S11Working:
    def __init__(self, sheet_name: str, urls: list, page_type: object):
        original_file_ = merge_file(
            sheet_name=sheet_name, urls=urls, page_type=page_type
        )
        if original_file_.empty:
            print("original_file is empty")
            pass
        else:
            self.original_file = original_file_
            self.sheet_name = sheet_name
            self.page_type = page_type

    def check_box(self):
        df = self.original_file
        s11_checkbox(df=df)
        update_s11_check_box(df=df)

    def s11_filter(self):
        df = self.original_file
        s11_checkbox(df=df)
        if s11_checkbox(df=df):
            filter_df = (
                df[
                    (df["itune_album_url"] != "not found")
                    & (df["itune_album_url"] != "")
                ]
                .drop_duplicates(
                    subset=["itune_album_url", "gsheet_info"], keep="first"
                )
                .reset_index()
            )

            filter_df["itune_id"] = filter_df["itune_album_url"].apply(
                lambda x: get_itune_id_region_from_itune_url(url=x)[0]
            )
            filter_df["region"] = filter_df["itune_album_url"].apply(
                lambda x: get_itune_id_region_from_itune_url(url=x)[1]
            )
            return filter_df
        else:
            pass

    def crawl_s11_datalake(self):
        df = self.s11_filter()
        if getattr(self.page_type, "name") == "NewClassic":
            is_new_release = True
        else:
            is_new_release = False
        if df.empty:
            print(Fore.LIGHTYELLOW_EX + f"s11 file is empty" + Style.RESET_ALL)
        else:
            df["query"] = df.apply(
                lambda x: crawl_itunes_album(
                    ituneid=x["itune_id"],
                    priority=get_key_value_from_gsheet_info(
                        gsheet_info=x["gsheet_info"], key="page_priority"
                    ),
                    is_new_release=is_new_release,
                    pic=f"{get_key_value_from_gsheet_info(gsheet_info=x['gsheet_info'], key='gsheet_name')}_{get_key_value_from_gsheet_info(gsheet_info=x['gsheet_info'], key='sheet_name')}",
                    region=x["region"],
                ),
                axis=1,
            )
        query_pandas_to_csv(df=df, column="query")

    def checking_s11_crawler_status(self):
        checking_s11_crawler_status(df=self.original_file)


class C11Working:
    def __init__(
        self, sheet_name: str, urls: list, page_type: object, pre_valid: str = None
    ):
        original_file_ = merge_file(
            sheet_name=sheet_name, urls=urls, page_type=page_type
        )
        if original_file_.empty:
            print("original_file is empty")
            pass
        else:
            self.original_file = original_file_
            self.sheet_name = sheet_name
            self.page_type = page_type
            self.pre_valid = pre_valid

    def pre_valid_(self):
        original_df = self.original_file
        gsheet_infos = original_df.gsheet_info.unique()
        for gsheet_info in gsheet_infos:
            url = get_key_value_from_gsheet_info(gsheet_info=gsheet_info, key="url")
            sheet_name = get_key_value_from_gsheet_info(gsheet_info, "sheet_name")
            print(sheet_name)
            sheet = get_worksheet(url, sheet_name)
            original_df_split = sheet.sheet_to_df(index=None)
            original_df_split.columns = original_df_split.columns.str.lower()
            print(original_df_split.columns)
            pointlogids = original_df_split[original_df_split["pointlogsid"] != ""][
                "pointlogsid"
            ].tolist()
            pointlogids_prevalid = get_df_from_query(
                get_pointlogsid_valid_for_contribution(pointlogids=pointlogids)
            )
            data_merge = pd.merge(
                original_df_split,
                pointlogids_prevalid,
                how="left",
                left_on="pointlogsid",
                right_on="id",
                validate="m:1",
            ).fillna(value=np.nan)
            current_date = f"{date.today()}"

            def replace_prevalid(valid):
                if valid == 0:
                    return current_date
                pass

            data_merge["pre_valid"] = data_merge["valid"].apply(replace_prevalid)
            if "pre_valid" in original_df_split.columns:
                col_index = original_df_split.columns.tolist().index("pre_valid")
                col_letter = string.ascii_uppercase[col_index]
                first_cell = f"{col_letter}2"
                last_cell = f"{col_letter}{data_merge.tail(1).index.item() + 2}"
                values = np.array(data_merge["pre_valid"]).T
                sheet.update_cells(first_cell, last_cell, vals=values)
            else:
                print("there is no pre_valid column")

    def check_box(self):
        df = self.original_file
        c11_checkbox(original_df=df, pre_valid=self.pre_valid)
        update_c11_check_box(original_df=df, pre_valid=self.pre_valid)

    def c11_filter(self):
        df = self.original_file
        if c11_checkbox(original_df=df, pre_valid=self.pre_valid):
            filter_df = df[
                (df["pre_valid"] == self.pre_valid)
                & (df["itune_album_url"] != "")
                & (~df["content type"].str.contains("REJECT"))
            ].reset_index()
            filter_df["itune_id"] = filter_df["itune_album_url"].apply(
                lambda x: get_itune_id_region_from_itune_url(url=x)[0]
            )
            filter_df["region"] = filter_df["itune_album_url"].apply(
                lambda x: get_itune_id_region_from_itune_url(url=x)[1]
            )
            filter_df = filter_df.drop_duplicates(
                subset=["itune_id", "gsheet_info"], keep="first"
            ).reset_index()
            return filter_df

    def crawl_c11_datalake(self):
        df = self.c11_filter()
        if getattr(self.page_type, "name") == "NewClassic":
            is_new_release = True
        else:
            is_new_release = False
        if df.empty:
            print(Fore.LIGHTYELLOW_EX + f"s11 file is empty" + Style.RESET_ALL)
        else:
            df["query"] = df.apply(
                lambda x: crawl_itunes_album(
                    ituneid=x["itune_id"],
                    priority=get_key_value_from_gsheet_info(
                        gsheet_info=x["gsheet_info"], key="page_priority"
                    ),
                    is_new_release=is_new_release,
                    pic=f"{get_key_value_from_gsheet_info(gsheet_info=x['gsheet_info'], key='gsheet_name')}_{get_key_value_from_gsheet_info(gsheet_info=x['gsheet_info'], key='sheet_name')}_{x['pre_valid']}",
                    region=x["region"],
                ),
                axis=1,
            )
        query_pandas_to_csv(df=df, column="query")
        data = [str(date.today()), "CY", "itunes_crawlalbum", len(df)]
        send_count_report(
            sheet_name="user_contribute", number_cols=4, data_to_insert=data
        )

    def checking_c11_crawler_status(self):
        checking_c11_crawler_status(
            original_df=self.original_file, pre_valid=self.pre_valid
        )

    def result_d9(self):
        result_d9(df=self.original_file, pre_valid=self.pre_valid)
        # send_message_slack("missing songs found from itunes",len(self.original_file[self.original_file['d9_status'] == 'complete']),cy_Itunes_plupdate,self.pre_valid).msg_slack()
        # send_message_slack(
        #     "missing songs found from itunes",
        #     len(self.original_file[(self.original_file["d9_status"] == "complete") & (self.original_file["pre_valid"] == self.pre_valid)]),
        #     cy_Itunes_plupdate,
        #     self.pre_valid,
        # ).send_to_slack()

    def update_d9(self):
        filter_df = self.original_file
        filter_df = filter_df[
            (
                (filter_df["pre_valid"] == pre_valid)
                # & (~filter_df['content type'].str.contains('REJECT'))
                # & (filter_df['track_id'] != 'not found')
            )
        ].reset_index()
        gsheet_info = list(set(filter_df.gsheet_info.tolist()))[0]
        gsheet_name = get_key_value_from_gsheet_info(
            gsheet_info=gsheet_info, key="gsheet_name"
        )
        sheet_name = get_key_value_from_gsheet_info(
            gsheet_info=gsheet_info, key="sheet_name"
        )
        PIC_taskdetail = f"{gsheet_name}_{sheet_name}_{pre_valid}"

        filter_df["hyperlink"] = ""
        for i in filter_df[
            "contribution_link"
        ]:  # chỉnh lại format của link youtube_url
            if len(i) < 43:
                filter_df["hyperlink"].loc[filter_df["contribution_link"] == i] = (
                    "https://www.youtube.com/watch?v=" + i.strip()[-11:]
                )
            else:
                filter_df["hyperlink"].loc[
                    filter_df["contribution_link"] == i
                ] = i.strip()[:43]

        df_similarity_recheck = filter_df[
            ((filter_df["similarity"] != "100") & (filter_df["recheck"] != "ok"))
            & (filter_df["similarity"] != "not found")
        ]

        criteria = {
            "LIVE_VIDEO": "live_concert_name_place",
            "OFFICIAL_MUSIC_VIDEO_2": "official_music_video_2",
            "COVER_VIDEO": "artist_name",
            "REMIX_VIDEO": "artist_name",
        }

        append_missing_df = pd.DataFrame()
        for content_type in criteria:
            df_value = filter_df[filter_df["content type"] == content_type][
                criteria.get(content_type)
            ]
            for value in df_value:
                if value.strip() == "":
                    missing_df = filter_df[
                        (filter_df["content type"] == content_type)
                        & (filter_df[criteria.get(content_type)] == value)
                    ]
                else:
                    missing_df = []
                append_missing_df = append_missing_df.append(missing_df)
        append_missing_df = append_missing_df.drop_duplicates(subset=None, keep="first")

        if df_similarity_recheck.empty and append_missing_df.empty:
            filter_df["crawling_task"] = filter_df.apply(
                lambda x: update_contribution(
                    content_type=x["content type"],
                    track_id=x["track_id"],
                    concert_live_name=x["live_concert_name_place"],
                    artist_name=x["artist_name"],
                    year=x["year"],
                    pic=PIC_taskdetail,
                    youtube_url=x["hyperlink"],
                    other_official_version=x["official_music_video_2"],
                    pointlogsid=x["pointlogsid"],
                ),
                axis=1,
            )

            count = len(
                filter_df[
                    ~filter_df["crawling_task"].isin(
                        ["-- content_type not existed", "", np.nan, None]
                    )
                ]
            )

            data = [str(date.today()), "CY", "itunes_verifypl", count]
            send_count_report(
                sheet_name="user_contribute", number_cols=4, data_to_insert=data
            )

            print(
                filter_df[
                    [
                        "pointlogsid",
                        "hyperlink",
                        "similarity",
                        "recheck",
                        "crawling_task",
                    ]
                ]
            )

            row_index = filter_df.index
            with open(query_path, "w") as f:
                for i in row_index:
                    line = filter_df["crawling_task"].loc[i]
                    # print(line)
                    f.write(f"{line}\n")
            f.close()
            print(
                Fore.LIGHTGREEN_EX
                + "Queries are printed out, please check"
                + Style.RESET_ALL
            )
        else:

            def missing_similarity():
                print(
                    Fore.LIGHTRED_EX
                    + "\nmissing similarity recheck as below\n"
                    + Style.RESET_ALL,
                    df_similarity_recheck[
                        ["pointlogsid", "hyperlink", "similarity", "recheck"]
                    ],
                )

            def missing_content_info():
                print(
                    Fore.LIGHTRED_EX
                    + "\nmissing info from content type/track_id as below\n"
                    + Style.RESET_ALL,
                    append_missing_df[
                        [
                            "pointlogsid",
                            "content type",
                            "official_music_video_2",
                            "artist_name",
                            "year",
                            "live_concert_name_place",
                        ]
                    ],
                )

            if append_missing_df.empty:
                missing_similarity()
            elif df_similarity_recheck.empty:
                missing_content_info()
            else:
                missing_similarity()
                missing_content_info()


class ControlFlow:
    def __init__(
        self, sheet_name: str, urls: list, page_type: object, pre_valid: str = ""
    ):
        self.page_type = page_type
        self.urls = urls
        self.sheet_name = sheet_name
        self.pre_valid = pre_valid

    def pre_valid_(self):
        if self.sheet_name == SheetNames.C_11:
            c11_working = C11Working(
                sheet_name=self.sheet_name,
                urls=self.urls,
                page_type=self.page_type,
                pre_valid=self.pre_valid,
            )
            return c11_working.pre_valid_()

    def update_d9(self):
        if self.sheet_name == SheetNames.C_11:
            c11_working = C11Working(
                sheet_name=self.sheet_name,
                urls=self.urls,
                page_type=self.page_type,
                pre_valid=self.pre_valid,
            )
            return c11_working.update_d9()

    def check_box(self):
        switcher = {
            SheetNames.ARTIST_IMAGE: ImageWorking,
            SheetNames.ALBUM_IMAGE: ImageWorking,
            SheetNames.MP3_SHEET_NAME: YoutubeWorking,
            SheetNames.MP4_SHEET_NAME: YoutubeWorking,
            SheetNames.S_11: S11Working,
            SheetNames.C_11: C11Working,
        }
        # get a list of all parameters to init a class for switcher[key]
        keys = signature(switcher[self.sheet_name]).parameters.keys()
        # get the values from self to initiate an instance based on the list of keys above
        attr = tuple([self.__dict__[i] for i in keys])
        # instantiate a class object using the values obtained
        check = switcher[self.sheet_name](*attr)
        return check.check_box()

    def observe(self):
        if self.sheet_name in (SheetNames.ARTIST_IMAGE, SheetNames.ALBUM_IMAGE):
            image_working = ImageWorking(
                sheet_name=self.sheet_name, urls=self.urls, page_type=self.page_type
            )
            return image_working.image_filter()
        elif self.sheet_name in (SheetNames.MP3_SHEET_NAME, SheetNames.MP4_SHEET_NAME):
            youtube_working = YoutubeWorking(
                sheet_name=self.sheet_name, urls=self.urls, page_type=self.page_type
            )
            return youtube_working.youtube_filter()
        elif self.sheet_name == SheetNames.S_11:
            s11_working = S11Working(
                sheet_name=self.sheet_name, urls=self.urls, page_type=self.page_type
            )
            return s11_working.s11_filter()
        elif self.sheet_name == SheetNames.C_11:
            c11_working = C11Working(
                sheet_name=self.sheet_name,
                urls=self.urls,
                page_type=self.page_type,
                pre_valid=self.pre_valid,
            )
            return c11_working.c11_filter()

    def crawl(self):
        if self.sheet_name in (SheetNames.ARTIST_IMAGE, SheetNames.ALBUM_IMAGE):
            image_working = ImageWorking(
                sheet_name=self.sheet_name, urls=self.urls, page_type=self.page_type
            )
            return image_working.crawl_image_datalake()

        elif self.sheet_name in (SheetNames.MP3_SHEET_NAME, SheetNames.MP4_SHEET_NAME):
            youtube_working = YoutubeWorking(
                sheet_name=self.sheet_name, urls=self.urls, page_type=self.page_type
            )
            return youtube_working.crawl_mp3_mp4_youtube_datalake()

        elif self.sheet_name == SheetNames.S_11:
            s11_working = S11Working(
                sheet_name=self.sheet_name, urls=self.urls, page_type=self.page_type
            )
            return s11_working.crawl_s11_datalake()

        elif self.sheet_name == SheetNames.C_11:
            c11_working = C11Working(
                sheet_name=self.sheet_name,
                urls=self.urls,
                page_type=self.page_type,
                pre_valid=self.pre_valid,
            )
            return c11_working.crawl_c11_datalake()

    def checking(self):
        if self.sheet_name in (SheetNames.ARTIST_IMAGE, SheetNames.ALBUM_IMAGE):
            image_working = ImageWorking(
                sheet_name=self.sheet_name, urls=self.urls, page_type=self.page_type
            )
            return image_working.checking_image_crawler_status()

        elif self.sheet_name in (SheetNames.MP3_SHEET_NAME, SheetNames.MP4_SHEET_NAME):
            youtube_working = YoutubeWorking(
                sheet_name=self.sheet_name, urls=self.urls, page_type=self.page_type
            )
            return youtube_working.checking_youtube_crawler_status()

        elif self.sheet_name == SheetNames.S_11:
            s11_working = S11Working(
                sheet_name=self.sheet_name, urls=self.urls, page_type=self.page_type
            )
            return s11_working.checking_s11_crawler_status()

        elif self.sheet_name == SheetNames.C_11:
            c11_working = C11Working(
                sheet_name=self.sheet_name,
                urls=self.urls,
                page_type=self.page_type,
                pre_valid=self.pre_valid,
            )
            return c11_working.checking_c11_crawler_status()

    def similarity(self):
        if self.sheet_name in (SheetNames.MP3_SHEET_NAME, SheetNames.MP4_SHEET_NAME):
            youtube_working = YoutubeWorking(
                sheet_name=self.sheet_name, urls=self.urls, page_type=self.page_type
            )
            return youtube_working.similarity()

    def result_d9(self):
        if self.sheet_name == SheetNames.C_11:
            c11_working = C11Working(
                sheet_name=self.sheet_name,
                urls=self.urls,
                page_type=self.page_type,
                pre_valid=self.pre_valid,
            )
            return c11_working.result_d9()


if __name__ == "__main__":
    start_time = time.time()

    pd.set_option(
        "display.max_rows", None, "display.max_columns", 30, "display.width", 500
    )
    with open(query_path, "w") as f:
        f.truncate()
    urls = [
<<<<<<< HEAD
        # "https://docs.google.com/spreadsheets/d/1XCtbHzP15FRduJzf_ena4tdye6oHwzpD-IRNdPV9jpM/edit#gid=328295841",
        # "https://docs.google.com/spreadsheets/d/1pEZBzBwmduhZYN9k5doNbuYW75NSSx-dEb_EHqu8Ysw/edit#gid=574925011",
        # "https://docs.google.com/spreadsheets/d/1Qinw5DaClvGLXGe22iw9_pRDuN2-zpxp5WD_c5-Lltg/edit#gid=1403431564",
        "https://docs.google.com/spreadsheets/d/1ExsBZA3043PKySiG1T4U9domUeUyn3j9bLi29XjgThY/edit#gid=2003688570"
    ]
    sheet_name_ = SheetNames.C_11
    page_type_ = PageType.Contribution
    pre_valid = "2021-07-06"
=======
    "https://docs.google.com/spreadsheets/d/1s1FTTguiLhJuMAJltMeolqx9eoA-nDKpIUqgsVJ3-eY/edit#gid=1284011554"
    ]
    sheet_name_ = SheetNames.MP4_SHEET_NAME
    page_type_ = PageType.NewClassic
    pre_valid = ""
>>>>>>> need sleep timer

    # control_flow = ControlFlow(
    #     sheet_name=sheet_name_, urls=urls, page_type=page_type_)
    # ControlFlow_C11
    control_flow = ControlFlow(
        sheet_name=sheet_name_, urls=urls, page_type=page_type_, pre_valid=pre_valid
    )

    # Contribution: pre_valid
    # control_flow.pre_valid_()

    # check_box:
    # control_flow.check_box()

    # observe:
    # k = control_flow.observe()
    # print(k)

    # similarity:
    control_flow.similarity()

    # crawl:
    control_flow.crawl()

    # checking
    # control_flow.checking()

    # update d9
    # control_flow.update_d9()

    # check d9_result
    # control_flow.result_d9()

    print("\n --- total time to process %s seconds ---" % (time.time() - start_time))
