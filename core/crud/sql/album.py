from core.models.album import Album
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import func, union, distinct, desc
from core.mysql_database_connection.sqlalchemy_create_engine import SQLALCHEMY_DATABASE_URI

from core.crud.sqlalchemy import get_compiled_raw_mysql

engine = create_engine(SQLALCHEMY_DATABASE_URI)
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

def get_album_wiki(albumuuid: tuple):
    album_wiki = (db_session.query(Album.uuid,
                                   Album.title,
                                   Album.artist,
                                   func.json_extract(Album.info, "$.wiki_url").label("wiki_url"),
                                   func.json_extract(Album.info, "$.wiki.brief").label("wiki_content")
                                   )
                .select_from(Album)
                .filter(Album.uuid.in_(albumuuid))
                )
    return album_wiki
