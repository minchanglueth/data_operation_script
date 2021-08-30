import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict

from core.models.base_class import Base, TimestampMixin


class ThemeAlbum(Base, TimestampMixin):
    __tablename__ = "theme_album"
    theme_id = sa.Column("ThemeId", sa.BigInteger, primary_key=True)
    album_id = sa.Column("AlbumId", sa.BigInteger)
    valid = sa.Column("Valid", sa.SmallInteger, nullable=False)
    display_order = sa.Column("DisplayOrder", sa.SmallInteger)
    ext = sa.Column("Ext", MutableDict.as_mutable(sa.JSON))

    def __str__(self) -> str:
        return self.theme_id
