import sqlalchemy as sa

from core.models.base_class import Base, TimestampMixin


class PlaylistDataSource(Base):
    __tablename__ = "essential_playlist_datasource"
    playlist_id = sa.Column("PlaylistId", sa.String(32), primary_key=True)
    datasource_id = sa.Column("DatasourceId", sa.String(32), primary_key=True)
    valid = sa.Column("Valid", sa.SmallInteger, nullable=False, default=1)
