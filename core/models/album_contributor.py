import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict

from core.models.base_class import Base, TimestampMixin


class AlbumContributor(Base, TimestampMixin):
    __tablename__ = "albumcontributors"
    album_id = sa.Column("AlbumId", sa.String(32), primary_key=True)
    user_id = sa.Column("UserId", sa.String(32), primary_key=True)
    valid = sa.Column("Valid", sa.SmallInteger, nullable=False, default=1)
    points = sa.Column("Points", sa.SmallInteger)
    ext = sa.Column("Ext", MutableDict.as_mutable(sa.JSON))
