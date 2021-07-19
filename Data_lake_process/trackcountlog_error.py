from gspread_dataframe import get_as_dataframe, set_with_dataframe
from gspread_formatting.dataframe import format_with_dataframe
from support_function.slack_function.slack_message import (
    trackcountlog_error_message,
    trackcountlog_error,
)
from core.models.crawlingtask import Crawlingtask
from core.models.datasource import DataSource
from core.models.trackcountlog import TrackCountLog
from core.models.track import Track
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker, aliased
from sqlalchemy import func, union, distinct, desc, and_, or_, tuple_, extract
from sqlalchemy import text, literal
from core.mysql_database_connection.sqlalchemy_create_engine import (
    SQLALCHEMY_DATABASE_URI,
)
from google_spreadsheet_api.gspread_utility import gc
from datetime import datetime, timedelta
from core.crud.get_df_from_query import get_df_from_query
import pandas as pd
import numpy as np

engine = create_engine(SQLALCHEMY_DATABASE_URI)
db_session = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine)
)


def query_datasource():
    query = (
        db_session.query(
            Track.id,
            Track.title,
            TrackCountLog.updated_at.label("trackcountlog_updated"),
            DataSource.updated_at.label("datasource_updated"),
        )
        .select_from(DataSource)
        .join(Track, and_(DataSource.track_id == Track.id, Track.valid == 1))
        .join(TrackCountLog, and_(TrackCountLog.track_id == Track.id))
        .filter(
            TrackCountLog.updated_at < DataSource.updated_at,
            DataSource.source_name == "spotify",
            DataSource.valid == 1,
            DataSource.updated_at > (datetime.now() - timedelta(hours=24)),
        )
        .order_by(DataSource.updated_at.desc())
    )
    # date = datetime.now().date()
    sh = gc.open_by_url(
        "https://docs.google.com/spreadsheets/d/1yJS1JjkaoNy2akdEbpTeQnKJgjji-1h9BHnFbyQ6XQc/edit#gid=0"
    )
    df_ = get_df_from_query(query)
    df_["script_date"] = datetime.now().date()
    worksheet = sh.get_worksheet(0)
    dff = get_as_dataframe(worksheet)
    df = pd.concat([df_, dff])
    set_with_dataframe(worksheet, df)
    format_with_dataframe(worksheet, df, include_column_header=True)
    return df


def query_crawlingtask():
    query = (
        db_session.query(
            Track.id,
            Track.title,
            TrackCountLog.updated_at.label("trackcountlog_updated"),
            Crawlingtask.updated_at.label("crawling_task_updated"),
            Crawlingtask.status.label("crawlingtask_status"),
        )
        .select_from(Crawlingtask)
        .join(Track, and_(Crawlingtask.objectid == Track.id, Track.valid == 1))
        .join(TrackCountLog, and_(TrackCountLog.track_id == Track.id))
        .filter(
            TrackCountLog.updated_at < Crawlingtask.updated_at,
            Crawlingtask.updated_at > (datetime.now() - timedelta(hours=24)),
        )
        .order_by(Crawlingtask.updated_at.desc())
    )
    date = datetime.now().date()
    sh = gc.open_by_url(
        "https://docs.google.com/spreadsheets/d/1yJS1JjkaoNy2akdEbpTeQnKJgjji-1h9BHnFbyQ6XQc/edit#gid=0"
    )
    df_ = get_df_from_query(query)
    df_["script_date"] = datetime.now().date()
    worksheet = sh.get_worksheet(1)
    dff = get_as_dataframe(worksheet)
    df = pd.concat([df_, dff])
    set_with_dataframe(worksheet, df)
    format_with_dataframe(worksheet, df, include_column_header=True)
    return df


if __name__ == "__main__":
    df = query_datasource()
    dff = query_crawlingtask()
    print(df.head())
