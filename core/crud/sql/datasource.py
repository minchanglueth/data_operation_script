from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import func, union, distinct, desc
from core.mysql_database_connection.sqlalchemy_create_engine import SQLALCHEMY_DATABASE_URI
from core.crud.sqlalchemy import get_compiled_raw_mysql

from core.models.datasource import DataSource
from core.models.playlist_datasource import PlaylistDataSource
from core.models.usernarrative import UserNarrative
from core.models.collection_datasource import CollectionDataSource
from core.models.data_source_format_master import DataSourceFormatMaster

from typing import Optional, Tuple, Dict, List
from core.crud.sqlalchemy import get_compiled_raw_mysql
from itertools import chain

from core.crud.get_df_from_query import get_df_from_query
import pandas as pd

engine = create_engine(SQLALCHEMY_DATABASE_URI)
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))


# def get_all_by_ids(data_source_id: list[str]) -> list[DataSource]:
#     return db_session.query(DataSource).filter((DataSource.id.in_(data_source_id)) & (DataSource.valid == 1)).all()


def get_datasourceids_from_youtube_url_and_trackid(youtube_url: str, trackid: str, formatid: str):
    datasourceid = (db_session.query(DataSource.id)
                    .select_from(DataSource)
                    .filter(DataSource.valid == 1,
                            DataSource.track_id == trackid,
                            DataSource.source_uri == youtube_url,
                            DataSource.format_id == formatid
                            )
                    )
    return datasourceid


def related_datasourceid(datasourceid: str):
    # Checking if exist in related_table
    datasourceid = (db_session.query(DataSource.id.label('datasourceid'), PlaylistDataSource.playlist_id,
                                     UserNarrative.id.label('narrative_id'),
                                     CollectionDataSource.collection_uuid)
                    .select_from(DataSource)
                    .outerjoin(PlaylistDataSource,
                               PlaylistDataSource.datasource_id == DataSource.id)
                    .outerjoin(UserNarrative,
                               UserNarrative.content_json.like("%" + DataSource.id + "%"))
                    .outerjoin(CollectionDataSource,
                               CollectionDataSource.datasource_id == DataSource.id)
                    .filter(DataSource.id == datasourceid)
                    )
    return datasourceid


def get_all_datasource_valid() -> List[DataSource]:
    return db_session.query(DataSource).filter((DataSource.valid == 1),
                                               DataSource.format_id == '74BA994CF2B54C40946EA62C3979DDA3').order_by(
        DataSource.created_at.desc()).all()


def get_all_datasource_by_ids(datasourceids: list):
    # Nho update lai sau khi chay xong lost S3 datasource
    return db_session.query(DataSource).filter((DataSource.valid == 1),
                                               DataSource.id.in_(datasourceids)).order_by(
        DataSource.updated_at.asc()).all()


def get_one_datasource_by_id(datasourceid: str):
    # Nho update lai sau khi chay xong lost S3 datasource
    db_datasource = db_session.query(DataSource).filter((DataSource.valid == 1),
                                               DataSource.id == datasourceid).order_by(
        DataSource.updated_at.asc()).all()
    if not db_datasource:
        return db_datasource
    else:
        return db_datasource[0]


def get_youtube_info_from_trackid(track_ids: list, format_id):
    datasourceid = (db_session.query(DataSource.track_id.label('track_id'),
                                     DataSource.id.label('datasource_id'),
                                     DataSource.source_uri.label('youtube_url'),
                                     func.json_extract(DataSource.info, "$.source.title").label(
                                         "youtube_title"),
                                     func.json_extract(DataSource.info, "$.source.uploader").label(
                                         "youtube_uploader"))
                    .select_from(DataSource)
                    .filter(DataSource.track_id.in_(track_ids),
                            DataSource.valid == 1,
                            DataSource.format_id == format_id)
                    ).group_by(DataSource.track_id).order_by(DataSource.created_at.desc())
    return datasourceid


def get_youtube_title_and_youtube_uploader_from_youtube_url(youtube_url: str):
    datasourceid = (db_session.query(
        DataSource.source_uri.label('youtube_url'),
        func.json_extract(DataSource.info, "$.source.title").label(
            "youtube_title"),
        func.json_extract(DataSource.info, "$.source.uploader").label(
            "youtube_uploader"))
                    .select_from(DataSource)
                    .filter(DataSource.source_uri == youtube_url,
                            DataSource.valid == 1
                            )
                    ).group_by(DataSource.track_id).order_by(DataSource.created_at.desc())
    return datasourceid


def get_one_youtube_url_and_youtube_uploader_by_youtube_url(youtube_url: str):
    return db_session.query(DataSource).filter((DataSource.valid == 1),
                                               func.json_extract(DataSource.info, "$.source.uploader") != None,
                                               func.json_extract(DataSource.info, "$.source.title") != None,
                                               DataSource.source_uri == youtube_url,
                                               func.json_extract(DataSource.info, "$.source.title") != "_",
                                               ).order_by(
        DataSource.created_at.desc()).limit(1).all()


def get_list_datasourceid():
     datasourceid = db_session.query(DataSource.id).filter(DataSource.valid == 1,
                                               DataSource.updated_at > '2020-10-01'
                                               ).order_by(
        DataSource.updated_at.desc())
     return datasourceid

def get_one_by_trackid_formatid(trackid: str, formatid: str):
    return db_session.query(DataSource).filter((DataSource.valid == 1),
                                               DataSource.track_id == trackid,
                                               DataSource.format_id == formatid,
                                               ).order_by(
        DataSource.created_at.desc()).first()

if __name__ == "__main__":
    track_ids = ["BDC6FC7C034446D993455E49B7F6C991"]


    # url = 'https://www.youtube.com/watch?v=RGiKQaiaVHE'
    # format_id = DataSourceFormatMaster.FORMAT_ID_MP3_FULL
    # joy = get_compiled_raw_mysql(get_one_youtube_url_and_youtube_uploader_by_youtube_url(url))
    print(joy)
