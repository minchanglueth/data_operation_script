import sqlalchemy as sa

from core.models.base_class import Base, TimestampMixin


class Artist_album(Base, TimestampMixin):
    __tablename__ = "Artist_Album"
    artist_id = sa.Column("ArtistId", sa.BigInteger, nullable=False, primary_key=True)
    album_id = sa.Column("AlbumId", sa.BigInteger, nullable=False, primary_key=True)
    priority = sa.Column("Priority", sa.SmallInteger, nullable=False, default=1)
    valid = sa.Column("Valid", sa.SmallInteger, nullable=False, default=1)

    def __str__(self):
        return f"{self.artist_id}-{self.album_id}"
