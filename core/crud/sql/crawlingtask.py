from core.models.crawlingtask import Crawlingtask
from core.models.artist import Artist
from core.models.album import Album
from core.models.datasource import DataSource
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker, aliased
from sqlalchemy import func, union, distinct, desc
from sqlalchemy import text
from core.mysql_database_connection.sqlalchemy_create_engine import SQLALCHEMY_DATABASE_URI
from datetime import date

from core.crud.sqlalchemy import get_compiled_raw_mysql

engine = create_engine(SQLALCHEMY_DATABASE_URI)
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))


def get_crawl_image_status(gsheet_name: str, sheet_name: str):
    crawl_artist_image_status = (db_session.query(Crawlingtask.id,
                                                  Crawlingtask.actionid,
                                                  Crawlingtask.objectid,
                                                  Crawlingtask.taskdetail,
                                                  Crawlingtask.status)
                                 .select_from(Crawlingtask)
                                 .filter(func.json_extract(Crawlingtask.taskdetail, "$.PIC") == f"{gsheet_name}_{sheet_name}",
                                         Crawlingtask.actionid == 'OA9CPKSUT6PBGI1ZHPLQUPQCGVYQ71S9')
                                 .order_by(Crawlingtask.objectid, Crawlingtask.created_at.desc())
                                 )
    return crawl_artist_image_status


def get_artist_image_cant_crawl(artistuuid: list):
    artist_image_cant_crawl = (db_session.query(Artist.name,
                                                            Artist.uuid,
                                                            func.json_extract(Crawlingtask.taskdetail, "$.url").label(
                                                                "image_url"),
                                                            Crawlingtask.status
                                                            )
                                           .select_from(Crawlingtask)
                                           .join(Artist,
                                                 Artist.uuid == Crawlingtask.objectid)
                                           .filter(func.DATE(Crawlingtask.created_at) == func.current_date(),
                                                   Crawlingtask.actionid == 'OA9CPKSUT6PBGI1ZHPLQUPQCGVYQ71S9',
                                                   Crawlingtask.objectid.in_(artistuuid)
                                                   )
                                           .order_by(Crawlingtask.objectid, Crawlingtask.created_at.desc())

                                           )
    return artist_image_cant_crawl


def get_album_image_cant_crawl(artistuuid: list):
    album_image_cant_crawl = (db_session.query(Album.title,
                                               Album.uuid,
                                               func.json_extract(Crawlingtask.taskdetail, "$.url").label(
                                                   "image_url"),
                                               Crawlingtask.status
                                               )
                              .select_from(Crawlingtask)
                              .join(Album,
                                    Album.uuid == Crawlingtask.objectid)
                              .filter(func.DATE(Crawlingtask.created_at) == func.current_date(),
                                      Crawlingtask.actionid == 'OA9CPKSUT6PBGI1ZHPLQUPQCGVYQ71S9',
                                      Crawlingtask.objectid.in_(artistuuid)
                                      )
                              .order_by(Crawlingtask.objectid, Crawlingtask.created_at.desc())
                              )
    return album_image_cant_crawl


def get_crawl_E5_06_status(ituneid: list):
    crawlingtask_alias = aliased(Crawlingtask, name='crawlingtask_alias')

    crawl_E5_06_status = (
        db_session.query(func.json_unquote(func.json_extract(Crawlingtask.taskdetail, "$.album_id")).label("ituneid"),
                         Crawlingtask.status.label('06_status'),
                         crawlingtask_alias.status.label('E5_status')
                         )
        .select_from(Crawlingtask)
        .outerjoin(crawlingtask_alias,
                   text("crawlingtask_alias.id = Crawlingtasks.ext ->> '$.itunes_track_task_id'"))
        .filter(func.DATE(Crawlingtask.created_at) == func.current_date(),
                Crawlingtask.actionid == '9C8473C36E57472281A1C7936108FC06',
                func.json_extract(Crawlingtask.taskdetail, "$.album_id").in_(ituneid)
                )
        )
    return crawl_E5_06_status


def get_datasourceId_from_crawlingtask():
    conditions2 = "datasources.TrackId = crawlingtasks.ObjectId and datasources.SourceURI = crawlingtasks.TaskDetail ->> '$.youtube_url' and datasources.FormatID = crawlingtasks.TaskDetail ->> '$.data_source_format_id' and datasources.Valid = 1"
    record = (db_session.query(Crawlingtask.objectid.label("track_id"),
                               DataSource.format_id,
                               func.json_extract(Crawlingtask.taskdetail, "$.youtube_url").label("youtube_url"),
                               DataSource.id.label("datasource_id"),
                               Crawlingtask.status.label("crawler_status"),
                               Crawlingtask.id.label("crawlingtask_id"),
                               func.json_extract(Crawlingtask.ext, "$.message").label("message")
                               )
              .select_from(Crawlingtask)
              .outerjoin(DataSource,
                         text(conditions2)
                          )
              .filter(
                      func.DATE(Crawlingtask.created_at) == func.current_date(),
                      # func.current_date(),
                      Crawlingtask.actionid == 'F91244676ACD47BD9A9048CF2BA3FFC1',
                      Crawlingtask.priority.in_([999, 10000])
                      )
              .group_by(Crawlingtask.objectid,func.json_extract(Crawlingtask.taskdetail, "$.youtube_url"),func.json_extract(Crawlingtask.taskdetail, "$.data_source_format_id"))
              )
    return record


if __name__ == "__main__":
    artistuuid = ['F241DAA56E76411592789860AE809F5F', 'F241DAA56E76411592789860AE809F5F']
    joy = get_crawl_image_status(gsheet_name="joy", sheet_name="joy xinh")
    k = get_compiled_raw_mysql(joy)
    print(k)
