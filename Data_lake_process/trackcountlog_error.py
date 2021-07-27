from gspread_dataframe import get_as_dataframe, set_with_dataframe
from gspread_formatting.dataframe import format_with_dataframe
from support_function.slack_function.slack_message_trackcountlog import (
    trackcountlog_error_message,
    trackcountlog_error,
)
from core.models.datasource import DataSource
from core.models.trackcountlog import TrackCountLog
from core.models.track import Track
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import and_
from core.mysql_database_connection.sqlalchemy_create_engine import (
    SQLALCHEMY_DATABASE_URI,
)
from google_spreadsheet_api.gspread_utility import gc
from datetime import datetime, timedelta
from core.crud.get_df_from_query import get_df_from_query
import pandas as pd

engine = create_engine(SQLALCHEMY_DATABASE_URI)
db_session = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine)
)


def query_datasource():
    formatid_list = [
        "3CF047F3B0F349B3A9A39CE7FDAB1DA6",
        "408EEAB1D3CF41F3941F62F97372184F",
        "74BA994CF2B54C40946EA62C3979DDA3",
        "745BFC108F41441CB01AD8178AB02D2B",
        "7F8B6CD82F28437888BD029EFDA1E57F",
        "BB423826E6FA4839BBB4DA718F483D18",
        "C417CF7DC12C4A21A0B3576871823156",
        "F5D2FE4A15FB405E988A7309FD3F9920",
    ]
    query = (
        db_session.query(
            Track.id,
            Track.title,
            TrackCountLog.updated_at.label("trackcountlog_updated"),
            DataSource.created_at.label("datasource_updated"),
            DataSource.format_id,
        )
        .select_from(DataSource)
        .join(Track, and_(DataSource.track_id == Track.id, Track.valid == 1))
        .join(TrackCountLog, and_(TrackCountLog.track_id == Track.id))
        .filter(
            TrackCountLog.updated_at < DataSource.created_at,
            DataSource.valid == 1,
            Track.valid == 1,
            DataSource.created_at > (datetime.now() - timedelta(hours=24)),
        )
        .order_by(DataSource.created_at.desc())
    )
    # date = datetime.now().date()
    gsheet_url = "https://docs.google.com/spreadsheets/d/1yJS1JjkaoNy2akdEbpTeQnKJgjji-1h9BHnFbyQ6XQc/edit#gid=133198295"
    sh = gc.open_by_url(gsheet_url)
    df_ = get_df_from_query(query)
    df_["script_date"] = datetime.now().date()
    df_ = df_[df_["format_id"].isin(formatid_list)][
        ["id", "title", "trackcountlog_updated",
         "datasource_updated", "script_date"]
    ]
    worksheet = sh.get_worksheet(0)
    dff = get_as_dataframe(worksheet)
    df = pd.concat([df_, dff])
    set_with_dataframe(worksheet, df)
    format_with_dataframe(worksheet, df, include_column_header=True)
    return len(df_)


def send_slack_report():
    track_error_datasource = query_datasource()
    gsheet_url = "https://docs.google.com/spreadsheets/d/1yJS1JjkaoNy2akdEbpTeQnKJgjji-1h9BHnFbyQ6XQc/edit#gid=133198295"
    slack = trackcountlog_error_message(
        trackcountlog_error,
        datetime.now().date(),
        gsheet_url,
        track_error_datasource,
    )
    if track_error_datasource > 0:
        slack.send_slack_error()
    else:
        slack.send_slack_report()


if __name__ == "__main__":
    query_datasource()
    # query_crawlingtask()#
    # send_slack_report()
