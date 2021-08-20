from gspread_dataframe import get_as_dataframe, set_with_dataframe
from gspread_formatting.dataframe import format_with_dataframe
from sqlalchemy.sql.expression import union_all
from support_function.slack_function.slack_message_trackcountlog import (
    trackcountlog_error_message,
    trackcountlog_error,
)
from core.models.datasource import DataSource
from core.models.trackcountlog import TrackCountLog
from core.models.track import Track
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker, aliased
from sqlalchemy import func, union, distinct, desc, and_, or_, tuple_, extract, update
from core.mysql_database_connection.sqlalchemy_create_engine import (
    SQLALCHEMY_DATABASE_URI,
)
from google_spreadsheet_api.gspread_utility import gc
from datetime import datetime, timedelta
from core.crud.get_df_from_query import get_df_from_query
import pandas as pd
import numpy as np
import sqlalchemy
import time

engine = create_engine(SQLALCHEMY_DATABASE_URI)
db_session = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine)
)


def query_datasource():
    formatid_list = [
        "1A67A5F1E0D84FB9B48234AE65086375",
        "3CF047F3B0F349B3A9A39CE7FDAB1DA6",
        "408EEAB1D3CF41F3941F62F97372184F",
        "74BA994CF2B54C40946EA62C3979DDA3",
        "7F8B6CD82F28437888BD029EFDA1E57F",
        "BB423826E6FA4839BBB4DA718F483D18",
        "F5D2FE4A15FB405E988A7309FD3F9920",
    ]
    query1 = (
        db_session.query(
            DataSource.updated_at.label("datasource_updatedat"),
            DataSource.created_at.label("datasource_createdat"),
            DataSource.track_id,
            DataSource.id.label("datasource_id"),
            DataSource.format_id,
            TrackCountLog.updated_at.label("trackcountlog_updatedat"),
        )
        .select_from(DataSource)
        .join(Track, and_(DataSource.track_id == Track.id))
        .join(TrackCountLog, and_(TrackCountLog.track_id == Track.id), isouter=True)
        .filter(
            Track.valid == 1,
            DataSource.valid == 1,
            DataSource.created_at > TrackCountLog.updated_at,
            DataSource.format_id.in_(formatid_list),
        )
    )
    subquery1 = (
        db_session.query(Track.id)
        .select_from(Track)
        .filter(Track.valid == 1)
        .distinct()
        .subquery()
    )
    subquery2 = (
        db_session.query(TrackCountLog.track_id).select_from(TrackCountLog).subquery()
    )
    query2 = (
        db_session.query(
            DataSource.created_at.label("datasource_createdat"),
            DataSource.updated_at.label("datasource_updatedat"),
            DataSource.track_id,
            DataSource.id.label("datasource_id"),
            DataSource.format_id,
            sqlalchemy.null().label("trackcountlog_updatedat"),
        )
        # .select_from(DataSource)
        .filter(
            DataSource.track_id.isnot(None),
            DataSource.track_id != "",
            DataSource.track_id.in_(subquery1),
            DataSource.track_id.notin_(subquery2),
            DataSource.valid == 1,
            DataSource.format_id.in_(formatid_list),
        )
    )
    query3 = query1.union(query2)
    df_ = get_df_from_query(query3)
    if len(df_) > 0:
        df_ = df_[df_["format_id"].isin(formatid_list)].sort_values(
            ["trackcountlog_updatedat", "datasource_updatedat", "datasource_createdat"],
            ascending=False,
        )
        df_["script_date"] = datetime.now().date()
    return df_


def update_sheet(df, gsheet_url, sheet_index):
    sh = gc.open_by_url(gsheet_url)
    worksheet = sh.get_worksheet(sheet_index)
    if sheet_index == 0:
        worksheet.clear()
        set_with_dataframe(worksheet, df)
        format_with_dataframe(worksheet, df, include_column_header=True)
        return True
    else:
        worksheet.add_rows(df.shape[0])
        set_with_dataframe(worksheet, df, include_column_header=False, row=worksheet.row_count+1)
        return True



def send_slack(df, gsheet_url):
    slack = trackcountlog_error_message(
        trackcountlog_error, datetime.now().date(), gsheet_url, len(df)
    )
    if len(df) > 0:
        slack.send_slack_error()
    else:
        slack.send_slack_report()


def change_valid_negative(df):
    datasource_ids = df["datasource_id"].values.tolist()
    update = (
        db_session.query(DataSource)
        .filter(DataSource.id.in_(datasource_ids))
        .update({"valid": -96}, synchronize_session="fetch")
    )
    db_session.commit()


def change_valid_positive(df):
    datasource_ids = df["datasource_id"].values.tolist()
    update = (
        db_session.query(DataSource)
        .filter(DataSource.id.in_(datasource_ids))
        .update({"valid": 1}, synchronize_session="fetch")
    )
    db_session.commit()


if __name__ == "__main__":
    start = time.time()
    gsheet_url = "https://docs.google.com/spreadsheets/d/1yJS1JjkaoNy2akdEbpTeQnKJgjji-1h9BHnFbyQ6XQc/edit#gid=133198295"
    df = query_datasource()
    print(df.head())
    update_sheet(df=df, gsheet_url=gsheet_url,sheet_index=0)
    if len(df) > 0:
        update_sheet(df=df, gsheet_url=gsheet_url,sheet_index=1)
        change_valid_negative(df)
        time.sleep(1500)
        change_valid_positive(df)
        time.sleep(1500)
        dff = query_datasource()
        update_sheet(dff, gsheet_url, 0)
        send_slack(dff, gsheet_url)
    else:
        send_slack(df, gsheet_url)
    print(
        "\n --- total time to process %s seconds ---"
        % (time.time() - start)
    )
