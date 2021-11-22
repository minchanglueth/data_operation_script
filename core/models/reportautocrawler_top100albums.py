import sqlalchemy as sa
import json
from sqlalchemy.ext.mutable import MutableDict

from core.models.base_class import Base, TimestampMixin

class ReportAutoCrawlerTop100Album(Base):
    __tablename__ = "reportautocrawler_top100albums"
    id = sa.Column("Id", sa.String(32), primary_key=True)
    genre = sa.Column("Genre", sa.String(256))
    rank = sa.Column("Rank", sa.SmallInteger)
    album_title = sa.Column("AlbumTitle", sa.String(512))
    itunes_album_id = sa.Column("ItunesAlbumId", sa.Integer, default=None)
    track_number = sa.Column("TrackNum", sa.Integer, nullable=False, default=1)
    verification = sa.Column("Verification", sa.String(32))
    artist_name = sa.Column("ArtistName", sa.String(256))
    artist_uuid = sa.Column("ArtistUUID", sa.String(32))
    track_title = sa.Column("TrackTitle", sa.String(512), default=None)
    track_id = sa.Column("TrackId", sa.String(32))
    datasources_existed = sa.Column(
        "DataSourcesExisted", MutableDict.as_mutable(sa.JSON)
    )
    ext = sa.Column("Ext", MutableDict.as_mutable(sa.JSON))

    def __str__(self):
        return str(self.id)
