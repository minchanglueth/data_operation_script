import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict

from core.models.base_class import Base, TimestampMixin


class CollectionAlbum(Base, TimestampMixin):
    __tablename__ = "collection_album"
    user_id = sa.Column("UserId", sa.BigInteger, primary_key=True)
    album_id = sa.Column("AlbumId", sa.BigInteger)
    prority = sa.Column("Priority", sa.SmallInteger)
    ext = sa.Column("Ext", MutableDict.as_mutable(sa.JSON))

    def __str__(self) -> str:
        return self.user_id
