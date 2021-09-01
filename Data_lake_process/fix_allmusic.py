from gspread_dataframe import get_as_dataframe, set_with_dataframe
from gspread_pandas import Spread, Client
from gspread_formatting.dataframe import format_with_dataframe
from sqlalchemy.orm.session import Session
from google_spreadsheet_api.gspread_utility import gc, get_df_from_gsheet
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import and_, func
from core.mysql_database_connection.sqlalchemy_create_engine import (
    SQLALCHEMY_DATABASE_URI,
)
from core.models.crawlingtask import Crawlingtask
from core.models.album_track import Album_Track
from core.models.album import Album
from core.models.artist_album import Artist_album
from core.models.external_identity import ExternalIdentity
from core.models.itunes_album_tracks_release import ItunesRelease
from core.models.pointlog import PointLog
from core.crud.get_df_from_query import get_df_from_query
import pandas as pd
from uuid import uuid4
import time

engine = create_engine(SQLALCHEMY_DATABASE_URI)
Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
db_session = Session()


def get_ituneid(gsheet_url: str):
    sheet = Spread(spread=gsheet_url, sheet="Allmusic_")
    df = sheet.sheet_to_df(index=None, header_rows=2)
    df.columns = ["_".join(tup).rstrip("_") for tup in df.columns.values]
    dff = df[(df["Apple ID_Apple ID"].notnull()) & (df["Apple ID_Apple ID"] != "")][
        ["albumuuid", "artistuuid", "Album Title", "Artist Name", "Apple ID_Apple ID"]
    ]
    dff.columns = [
        "albumuuid_old",
        "artistuuid_old",
        "album_title",
        "artist_name",
        "apple_id",
    ]
    dff = dff.dropna(subset=["apple_id"])
    dff["apple_id"] = dff["apple_id"].astype(int)
    return dff


def run_crawler(df: object):
    itunes_id_list = df["apple_id"].values.tolist()
    crawler_actionid = "9C8473C36E57472281A1C7936108FC06"
    crawler_id_list = []
    insert_list = []
    for id in itunes_id_list:
        task_detail = {"PIC": "Maddie-allmusic", "region": "us", "album_id": str(id)}
        crawler_id = str(uuid4()).upper().replace("-", "")
        c = Crawlingtask(
            id=crawler_id,
            priority=1205,
            actionid=crawler_actionid,
            taskdetail=task_detail,
        )
        crawler_id_list.append(crawler_id)
        insert_list.append(c)

    db_session.bulk_save_objects(insert_list)
    db_session.commit()
    return crawler_id_list


def get_complete_crawl(id_list: list):
    query_crawler = (
        db_session.query(
            Crawlingtask.id,
            func.json_extract(Crawlingtask.taskdetail, "$.album_id").label("itune_id"),
        )
        .select_from(Crawlingtask)
        .filter(Crawlingtask.id.in_(id_list), Crawlingtask.status == "complete")
    )

    dfc = get_df_from_query(query_crawler)
    print(dfc.info())
    itunes_id_list = (
        dfc[dfc["itune_id"].notnull()]["itune_id"]
        .str.replace('"','')
        .astype(float)
        .astype(int)
        .values.tolist()
    )
    query_itune_id = db_session.query(Album.id, Album.uuid, Album.external_id).filter(
        Album.external_id.in_(itunes_id_list)
    )
    return query_itune_id


def get_new_album_id(query):
    df = get_df_from_query(query)
    new_album_ids = df["id"].values.tolist()
    return new_album_ids


def get_new_album_uuid(query):
    df = get_df_from_query(query)
    new_album_uuids = df["uuid"].values.tolist()
    return new_album_uuids


def merge_new_old_ids(query, df):
    new_df = get_df_from_query(query)
    merged_df = pd.merge(df, new_df, left_on="apple_id", right_on="external_id")
    return merged_df


if __name__ == "__main__":
    gsheet_url = "https://docs.google.com/spreadsheets/d/1H0t9xq2vUesfpBQoieJP6TxHbWl8D43s5kiAZ7K3Cgc/edit#gid=978085935"
    df = get_ituneid(gsheet_url)
    id_list = run_crawler(df)
    time.sleep(900)
    query = get_complete_crawl(id_list)
    merged_df = merge_new_old_ids(query=query, df=df)
    print(merged_df.head())
