import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict
from core.models.base_class import Base, TimestampMixin
class Track(Base, TimestampMixin):
    __tablename__ = "Tracks"
    id = sa.Column("Id", sa.String(32), primary_key=True)
    valid = sa.Column("Valid", sa.SmallInteger, nullable=False, default=1)
    name = sa.Column("Name", sa.String(256))
    title = sa.Column("Title", sa.String(512))
    artist = sa.Column("Artist", sa.String(512))
    duration_ms = sa.Column("DurationMs", sa.Integer)
    popularity = sa.Column("Popularity", sa.Integer, default=0)
    lyrics = sa.Column("Lyrics", sa.UnicodeText)
    review = sa.Column("Review", sa.UnicodeText)
    image_url = sa.Column("ImageURL", sa.String(1024))
    info = sa.Column("Info", MutableDict.as_mutable(sa.JSON))
    ext = sa.Column("Ext", MutableDict.as_mutable(sa.JSON))


