from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import func, union, distinct, desc
from core.models.genre import Genre
from core.mysql_database_connection.sqlalchemy_create_engine import SQLALCHEMY_DATABASE_URI
from core.crud.sqlalchemy import get_compiled_raw_mysql

engine = create_engine(SQLALCHEMY_DATABASE_URI)
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))


def get_genre_uuid_from_genre_name(genre_name: list):
    genre_uuid_from_genre_name = (db_session.query(Genre.uuid.label('genre_uuid'), Genre.title.label('genre_name'))
                              .select_from(Genre)
                              .filter(Genre.valid == 1,
                                      Genre.title.in_(genre_name))
                              )
    return genre_uuid_from_genre_name

if __name__ == "__main__":
    username = ['Rock','Pop']
    joy = get_compiled_raw_mysql(get_genre_uuid_from_genre_name(username))
    print(joy)
