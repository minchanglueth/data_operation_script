import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict

from core.models.base_class import Base, TimestampMixin


class TrackCountLog(Base, TimestampMixin):
    # noinspection SpellCheckingInspection
    __tablename__ = "TrackCountLog"
    track_id = sa.Column(
        "TrackID", sa.String(32), unique=True, nullable=False, primary_key=True
    )
    data_source_count = sa.Column("DataSourceCount", MutableDict.as_mutable(sa.JSON))
    percentage_count = sa.Column("PercentageCount", MutableDict.as_mutable(sa.JSON))
