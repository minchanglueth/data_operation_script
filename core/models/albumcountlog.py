import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict

from core.models.base_class import Base, TimestampMixin


class AlbumCountLog(Base, TimestampMixin):
    # noinspection SpellCheckingInspection
    __tablename__ = "albumcountlog"
    album_uuid = sa.Column(
        "AlbumUUID", sa.String(32), unique=True, nullable=False, primary_key=True
    )
    track_count = sa.Column("TrackCount", MutableDict.as_mutable(sa.JSON))
    data_source_count = sa.Column("DataSourceCount", MutableDict.as_mutable(sa.JSON))
    percentage_count = sa.Column("PercentageCount", MutableDict.as_mutable(sa.JSON))
