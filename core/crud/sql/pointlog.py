from core.models.pointlog import PointLog
from core.models.album_track import Album_Track
from core.models.track import Track
from core.models.album import Album
from core.models.crawlingtask import Crawlingtask
from core.models.datasource import DataSource
from core.crud.sqlalchemy import get_compiled_raw_mysql

from sqlalchemy import text
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import func, union, distinct, desc
from core.mysql_database_connection.sqlalchemy_create_engine import SQLALCHEMY_DATABASE_URI

engine = create_engine(SQLALCHEMY_DATABASE_URI)
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))


def collect_from_youtube_query():
    collect_from_youtube_query = (db_session.query(PointLog.created_at,
                                                   PointLog.valid,
                                                   PointLog.id,
                                                   func.json_extract(PointLog.info, "$.email").label("email"),
                                                   func.json_extract(PointLog.info, "$.youtube_url").label(
                                                       "contribution_url"),
                                                   Album_Track.track_id,
                                                   Track.title,
                                                   Track.artist.label("track_artist"),
                                                   Album.title.label("album_title"),
                                                   Album.valid.label("album_valid"),
                                                   Track.valid.label("track_valid"),
                                                   func.json_extract(PointLog.info, "$.content_type").label(
                                                       "contribution_type"),
                                                   func.json_extract(PointLog.info, "$.comment").label(
                                                       "contribution_comment"),
                                                   PointLog.crawler_status,
                                                   func.json_extract(PointLog.ext, "$.crawler_id").label("crawler_id"),
                                                   Crawlingtask.status,
                                                   PointLog.action_type,
                                                   func.json_extract(DataSource.info, "$.source.title").label(
                                                       "youtube_title"),
                                                   func.json_extract(DataSource.info, "$.source.uploader").label(
                                                       "youtube_uploader")
                                                   )
                                  .select_from(PointLog)
                                  .outerjoin(Crawlingtask,
                                             text("crawlingtasks.id = pointlogs.ext ->> '$.crawler_id'"))
                                  .outerjoin(Album_Track,
                                             (Album_Track.track_id == PointLog.target_id))
                                  .outerjoin(Album,
                                             (Album.uuid == Album_Track.album_uuid) & (Album.valid == 1))
                                  .outerjoin(Track,
                                             (Track.id == Album_Track.track_id) & (Track.id != "") & (Track.valid == 1))
                                  .outerjoin(DataSource,
                                             text("datasources.id = crawlingtasks.ext ->> '$.data_source_id'"))
                                  .filter(PointLog.action_type == "CY",
                                          PointLog.created_at > '2020-10-01',
                                          PointLog.valid == 0
                                          )
                                  .group_by(PointLog.id)
                                  .order_by(PointLog.created_at.desc())
                                  # .all()
                                  )
    return collect_from_youtube_query


def get_cutoff_date_collect_from_youtube():
    get_date = (db_session.query(PointLog.created_at)
                .select_from(PointLog)
                .filter(PointLog.valid != 0,
                        PointLog.action_type == "CY")
                .order_by(PointLog.created_at.desc())
                # .first()
                )
    return get_date


if __name__ == "__main__":
    joy = get_compiled_raw_mysql(collect_from_youtube_query())
    print(joy)
