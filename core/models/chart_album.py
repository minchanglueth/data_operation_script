import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict

from core.models.base_class import Base, TimestampMixin


class ChartAlbum(Base, TimestampMixin):
    __tablename__ = "Chart_album"
    chart_id = sa.Column("ChartId", sa.BigInteger, primary_key=True)
    album_id = sa.Column("AlbumId", sa.BigInteger)
    valid = sa.Column("Valid", sa.SmallInteger, nullable=False)
    order = sa.Column("Order", sa.SmallInteger)
    ext = sa.Column("Ext", MutableDict.as_mutable(sa.JSON))

    def __str__(self) -> str:
        return self.chart_id
