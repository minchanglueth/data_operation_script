from core.models.crawlingtask import Crawlingtask
from core.models.artist import Artist
from core.models.album import Album
from core.models.datasource import DataSource
from core.models.data_source_format_master import DataSourceFormatMaster
from core.models.crawlingtask_action_master import V4CrawlingTaskActionMaster
from core.models.itunes_album_tracks_release import ItunesRelease
from core.models.track import Track

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker, aliased
from sqlalchemy import func, union, distinct, desc, and_, or_, tuple_, extract
from sqlalchemy import text
from core.mysql_database_connection.sqlalchemy_create_engine import SQLALCHEMY_DATABASE_URI
from typing import Optional, Tuple, Dict, List

from datetime import date
import time

from core.crud.sqlalchemy import get_compiled_raw_mysql

engine = create_engine(SQLALCHEMY_DATABASE_URI)
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))


def count_datasource_by_artistname_formatid(artist_name: str, formatid: str):
    count_datasource_by_artistname_formatid = (db_session.query(
        ItunesRelease.album_uuid,
        ItunesRelease.itunes_url,
        ItunesRelease.track_seq,
        ItunesRelease.duration,
        Track.id.label("trackid"),
        DataSource.id.label("Datasourceid")
    )
                                               .select_from(ItunesRelease)
                                               .join(Track, and_(Track.title == ItunesRelease.track_title,
                                                                 Track.artist == ItunesRelease.track_artist,
                                                                 Track.valid == 1))
                                               .join(DataSource, and_(DataSource.track_id == Track.id,
                                                                      DataSource.valid == 1,
                                                                      DataSource.format_id == formatid,
                                                                      DataSource.format_id != '',
                                                                      DataSource.source_name == 'YouTube'))
                                               .filter(or_(ItunesRelease.album_artist.like("%" + artist_name + "%"),
                                                           ItunesRelease.track_artist.like("%" + artist_name + "%")))
                                               .group_by(Track.id, DataSource.id)
                                               ).all()
    count = len(count_datasource_by_artistname_formatid)
    return count


def get_datasource_by_artistname_formatid(artist_name: str, formatid: str):
    get_datasource_by_artistname_formatid = (db_session.query(
        ItunesRelease.album_uuid,
        ItunesRelease.album_title,
        ItunesRelease.album_artist,
        ItunesRelease.itunes_url,
        ItunesRelease.track_seq,
        ItunesRelease.duration,
        (extract('hour', ItunesRelease.duration) * 3600000 + extract('minute', ItunesRelease.duration) * 60000 + extract('second', ItunesRelease.duration) *1000).label("DurationMs"),
        Track.id.label("trackid"),
        Track.title.label("track_title"),
        Track.artist.label("track_artist"),
        DataSource.id.label("datasource_id"),
        DataSource.format_id.label("FormatID"),
        DataSource.source_uri.label("SourceURI")
    )
                                             .select_from(ItunesRelease)
                                             .join(Track, and_(Track.title == ItunesRelease.track_title,
                                                               Track.artist == ItunesRelease.track_artist,
                                                               Track.valid == 1))
                                             .join(DataSource, and_(DataSource.track_id == Track.id,
                                                                    DataSource.valid == 1,
                                                                    DataSource.format_id == formatid,
                                                                    DataSource.format_id != '',
                                                                    DataSource.source_name == 'YouTube'))
                                             .filter(or_(ItunesRelease.album_artist.like("%" + artist_name + "%"),
                                                         ItunesRelease.track_artist.like("%" + artist_name + "%")))
                                             .order_by(Track.title.asc(),Track.artist, ItunesRelease.album_uuid, ItunesRelease.track_seq.asc())
                                             .group_by(Track.id, DataSource.id)
                                             )
    return get_datasource_by_artistname_formatid

def get_crawlingtask_youtube_info(objectid: str, PIC: str, actionid: str):
    get_crawlingtask_info = (db_session.query(
        Crawlingtask.id,
        Crawlingtask.objectid,
        func.json_extract(Crawlingtask.taskdetail, "$.youtube_url").label(
            "youtube_title"),
        func.json_extract(Crawlingtask.taskdetail, "$.when_exists").label(
            "when_exists"),
        func.json_extract(Crawlingtask.taskdetail, "$.data_source_format_id").label(
            "data_source_format_id"),
        Crawlingtask.status

    )
                             .select_from(Crawlingtask)
                             .filter(Crawlingtask.objectid == objectid,
                                     Crawlingtask.actionid == actionid,
                                     func.json_extract(Crawlingtask.taskdetail, "$.PIC") == PIC,
                                     )
                             .order_by(
                                       Crawlingtask.created_at.desc())
                             ).first()
    return get_crawlingtask_info


def get_crawlingtask_info(objectid: str, PIC: str, actionid: str):
    get_crawlingtask_info = (db_session.query(
        Crawlingtask.id,
        Crawlingtask.objectid,
        func.json_extract(Crawlingtask.taskdetail, "$.url").label(
            "url"),
        func.json_extract(Crawlingtask.taskdetail, "$.when_exists").label(
            "when_exists"),
        Crawlingtask.status

    )
                             .select_from(Crawlingtask)
                             .filter(Crawlingtask.objectid == objectid,
                                     Crawlingtask.actionid == actionid,
                                     func.json_extract(Crawlingtask.taskdetail, "$.PIC") == PIC,
                                     )
                             .order_by(
                                       Crawlingtask.created_at.desc())
                             ).first()
    return get_crawlingtask_info


def get_crawlingtask_image_status(gsheet_name: str, sheet_name: str):

    crawl_artist_image_status = (db_session.query(
        Crawlingtask.id,
        Crawlingtask.objectid,
        func.json_extract(Crawlingtask.taskdetail, "$.url").label(
            "url"),
        func.json_extract(Crawlingtask.taskdetail, "$.when_exists").label(
            "when_exists"),
        Crawlingtask.status
    )
                                 .select_from(Crawlingtask)
                                 .filter(
        func.json_extract(Crawlingtask.taskdetail, "$.PIC") == f"{gsheet_name}_{sheet_name}",
        Crawlingtask.actionid == 'OA9CPKSUT6PBGI1ZHPLQUPQCGVYQ71S9')
                                 .order_by(Crawlingtask.objectid, Crawlingtask.created_at.desc())
                                 )
    return crawl_artist_image_status





if __name__ == "__main__":
    start_time = time.time()
# #     Artist Page 30.12.2020_MP_3---204F065101834F11BC74251C64967ECF---F91244676ACD47BD9A9048CF2BA3FFC1
    db_crawlingtask = get_crawlingtask_info(objectid="9587370BB39A4253B5F4381B7C9BD644",
                                            PIC="Top 100 Albums 08.03.2021_08.03.2021",
                                            actionid="OA9CPKSUT6PBGI1ZHPLQUPQCGVYQ71S9")
    print(db_crawlingtask.id)
#     print(k)
#     print("--- %s seconds ---" % (time.time() - start_time))
