import sqlalchemy as sa

from core.models.base_class import Base


# noinspection PyPep8Naming
class Album_Track(Base):
    __tablename__ = "Album_Track"
    album_uuid = sa.Column("AlbumId", sa.String(32), nullable=False, primary_key=True)
    track_id = sa.Column("TrackId", sa.String(32), nullable=False, primary_key=True)
    disc_number = sa.Column("DiscNumber", sa.SmallInteger, default=None)
    track_number = sa.Column(
        "TrackNumber", sa.SmallInteger, nullable=False, default=1, primary_key=True
    )
    album_priority = sa.Column("AlbumPriority", sa.SmallInteger, default=1)
