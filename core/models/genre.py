import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict

from core.models.base_class import Base, TimestampMixin


class Genre(Base, TimestampMixin):
    __tablename__ = "Genres"
    id = sa.Column("Id", sa.BigInteger, primary_key=True)
    uuid = sa.Column("UUID", sa.String(32), unique=True, nullable=False)
    valid = sa.Column("Valid", sa.SmallInteger, nullable=False, default=1)
    parent_id = sa.Column("ParentId", sa.BigInteger)
    title = sa.Column("Title", sa.String(128), nullable=False)
    ext = sa.Column("Ext", MutableDict.as_mutable(sa.JSON))
    artists = sa.Column("Artists", MutableDict.as_mutable(sa.JSON))
