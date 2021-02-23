from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import func, union, distinct, desc
from core.models.external_identity import ExternalIdentity
from core.models.album import Album
from core.models.artist_album import Artist_album
from core.models.artist import Artist
from core.models.itunes_album_tracks_release import ItunesRelease
from core.models.track import Track
from core.mysql_database_connection.sqlalchemy_create_engine import SQLALCHEMY_DATABASE_URI
from core.crud.sqlalchemy import get_compiled_raw_mysql

engine = create_engine(SQLALCHEMY_DATABASE_URI)
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))


def get_artists_from_album_ituneid(album_ituneid: list):
    sub_query = (db_session.query(ExternalIdentity.uuid.label('album_uuid')).distinct(ExternalIdentity.uuid)
                 .filter(ExternalIdentity.external_id.in_(album_ituneid))
                 .subquery()
                 )

    artists_from_album_ituneid = (
        db_session.query(Album.uuid.label('album_uuid'), Artist.uuid.label('artist_uuid'), Artist.name,
                         ExternalIdentity.external_id)
            .select_from(Album)
            .outerjoin(Artist_album,
                       (Artist_album.album_id == Album.id)
                       & (Album.valid == 1)
                       )
            .outerjoin(Artist,
                       (Artist.id == Artist_album.artist_id)
                       & (Artist.valid == 1)
                       )
            .outerjoin(ExternalIdentity,
                       (ExternalIdentity.uuid == Artist.uuid)
                       & (Artist.valid == 1)
                       )
            .filter(Album.uuid.in_(sub_query))
            .order_by(Album.uuid, Artist.name)
    )
    return artists_from_album_ituneid


def get_trackid_from_ituneid_and_tracknum(album_ituneid: list, track_num: list ):
    trackid_from_ituneid_and_tracknum = (db_session.query(ExternalIdentity.external_id.label('ituneid'),
                                                          ItunesRelease.track_seq.label('tracknum'),
                                                          Track.id.label('track_id'),
                                                          Track.title.label('track_title'),
                                                          Track.artist.label('track_artist')
                                                          )
                                         .select_from(ExternalIdentity)
                                         .outerjoin(ItunesRelease,
                                                    (ItunesRelease.album_uuid == ExternalIdentity.uuid)
                                                    )
                                         .outerjoin(Track,
                                                    (Track.title == ItunesRelease.track_title)
                                                    & (Track.artist == ItunesRelease.track_artist)
                                                    & (Track.valid == 1)
                                                    )
                                         .filter(ExternalIdentity.external_id.in_(album_ituneid),ItunesRelease.track_seq.in_(track_num))
                                         )
    return trackid_from_ituneid_and_tracknum


if __name__ == "__main__":
    # album_ituneid = '317186212'
    # track_num = '10'
    # joy = get_compiled_raw_mysql(get_trackid_from_ituneid_and_tracknum(album_ituneid, track_num))
    album_ituneid = ['joy', 'jay']
    track_num = [1,2]
    joy = get_compiled_raw_mysql(get_trackid_from_ituneid_and_tracknum(album_ituneid,track_num))
    print(joy)
