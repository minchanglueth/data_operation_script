import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict
from core.models.base_class import Base, TimestampMixin
class Artist(Base, TimestampMixin):
    __tablename__ = "Artists"
    uuid = sa.Column("UUID", sa.String(32), primary_key=True)
    id = sa.Column("Id", sa.BigInteger)
    valid = sa.Column("Valid", sa.SmallInteger, nullable=False, server_default="1")
    name = sa.Column("Name", sa.String(128), nullable=False)
    info = sa.Column("Info", MutableDict.as_mutable(sa.JSON))
    ext = sa.Column("Ext", MutableDict.as_mutable(sa.JSON))
    view_count = sa.Column("ViewCount", sa.BigInteger, server_default="0")
    video_count = sa.Column("VideoCount", sa.BigInteger, server_default="0")



