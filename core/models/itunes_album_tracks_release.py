import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict

from core.models.base_class import Base, TimestampMixin


class ItunesRelease(Base, TimestampMixin):
    __tablename__ = "itunes_album_tracks_release"
    id = sa.Column("Id", sa.BigInteger, primary_key=True, autoincrement=True)
    valid = sa.Column("Valid", sa.SmallInteger, nullable=False, default=1)
    album_uuid = sa.Column("AlbumUUID", sa.String(32), nullable=False)
    itunes_url = sa.Column("iTunesUrl", sa.String(512), default=None)
    itunes_album_id = sa.Column("ItuneAlbumId", sa.Integer, default=None)
    album_title = sa.Column("AlbumName", sa.String(512), default=None)
    track_number = sa.Column("TrackNumber", sa.Integer, nullable=False, default=1)
    track_seq = sa.Column("Seq", sa.Integer, nullable=False, default=1)
    track_title = sa.Column("TrackName", sa.String(512), default=None)
    album_artist = sa.Column("Artist", sa.String(512), default=None)
    track_artist = sa.Column("TrackArtist", sa.String(512), default=None)
    duration = sa.Column("Duration", sa.String(32), default=None)
    source = sa.Column("Source", sa.String(8))
    ext = sa.Column("Ext", MutableDict.as_mutable(sa.JSON))

    def __str__(self):
        return str(self.id)
