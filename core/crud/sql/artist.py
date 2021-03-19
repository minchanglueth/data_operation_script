from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import func, union, distinct, desc
from core.models.artist import Artist
from core.mysql_database_connection.sqlalchemy_create_engine import SQLALCHEMY_DATABASE_URI
from core.crud.sqlalchemy import get_compiled_raw_mysql

engine = create_engine(SQLALCHEMY_DATABASE_URI)
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))


def get_uuid_and_count_from_artist_name(artist_name: list):
    artist_uuid_from_artist_name = (db_session.query(Artist.name.label('artist_name'), Artist.uuid.label('artist_uuid'),
                                                     func.count(Artist.uuid).label('count_uuid_by_name'))
                                    .select_from(Artist)
                                    .filter(Artist.valid == 1,
                                            Artist.name.in_(artist_name))
                                    .group_by(Artist.name)
                                    )

    return artist_uuid_from_artist_name


def get_all_by_ids(artist_uuids: list):
    return db_session.query(Artist).filter((Artist.valid == 1),
                                           Artist.uuid.in_(artist_uuids)).order_by(Artist.created_at.desc()).all()


def get_one_by_id(artist_uuid: str):
    return db_session.query(Artist).filter((Artist.valid == 1),
                                           Artist.uuid == artist_uuid).order_by(Artist.created_at.desc()).first()


if __name__ == "__main__":
    artist_uuids = ['8A584B04D8094A47AAC757252B9FD4AA', 'D1F90D01CB164FD5B970D06EB0E7033B']
    # joy = get_compiled_raw_mysql(get_all_by_ids(artist_uuids))
    artist_uuid = "8A584B04D8094A47AAC757252B9FD4AA"
    joy = get_compiled_raw_mysql(get_one_by_id(artist_uuid))

    # print(joy)

# def get_all_by_ids(datasourceids: list):
