import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict

from core.models.base_class import Base, TimestampMixin


class RelatedAlbum(Base, TimestampMixin):
    __tablename__ = "relatedalbums"
    valid = sa.Column("Valid", sa.SmallInteger, nullable=False)
    related_album_id = sa.Column("RelatedAlbumId", sa.String(32), primary_key=True)
    album_id = sa.Column("AlbumId", sa.String(32), primary_key=True)
    difference = sa.Column("Difference", sa.Float)

    def __str__(self) -> str:
        return self.related_album_id
