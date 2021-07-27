from gspread_dataframe import get_as_dataframe, set_with_dataframe
from gspread_pandas import Spread, Client
from gspread_formatting.dataframe import format_with_dataframe
from google_spreadsheet_api.gspread_utility import gc, get_df_from_gsheet
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import and_
from core.mysql_database_connection.sqlalchemy_create_engine import (
    SQLALCHEMY_DATABASE_URI,
)
from core.crud.get_df_from_query import get_df_from_query
from core.models.crawlingtask import Crawlingtask
from core.models.album_track import Album_Track
from core.models.album import Album
from core.models.artist_album import Artist_album
from core.models.external_identity import ExternalIdentity
from core.models.itunes_album_tracks_release import ItunesRelease
from core.models.pointlog import PointLog
import pandas as pd
from uuid import uuid4

engine = create_engine(SQLALCHEMY_DATABASE_URI)
db_session = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine)
)


def get_ituneid(gsheet_url: str):
    sheet = Spread(spread=gsheet_url, sheet="Allmusic_")
    df = sheet.sheet_to_df(index=None, header_rows=2)
    df.columns = ["_".join(tup).rstrip("_") for tup in df.columns.values]
    dff = df[(df["Apple ID_Apple ID"].notnull()) & (df["Apple ID_Apple ID"] != "")][
        ["albumuuid", "artistuuid", "Album Title", "Artist Name", "Apple ID_Apple ID"]
    ]
    dff.columns = ["albumuuid", "artistuuid", "album_title", "artist_name", "apple_id"]
    print(dff.head())
    return dff


def run_crawler(df: object):
    itunes_id_list = df["apple_id"].values.tolist()
    crawler_actionid = "9C8473C36E57472281A1C7936108FC06"
    for id in itunes_id_list:
        task_detail = {"PIC": "Maddie", "region": "us", "album_id": id}
        c = Crawlingtask(
            id=str(uuid4()),
            priority=1205,
            actionid=crawler_actionid,
            taskdetail=task_detail
        )
        db_session.add(c)
    
    db_session.commit()


if __name__ == "__main__":
    gsheet_url = "https://docs.google.com/spreadsheets/d/1H0t9xq2vUesfpBQoieJP6TxHbWl8D43s5kiAZ7K3Cgc/edit#gid=978085935"
    df = get_ituneid(gsheet_url)
    run_crawler(df)
