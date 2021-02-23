import sqlalchemy as sa

from core.models.base_class import Base, TimestampMixin


class ExternalIdentity(Base, TimestampMixin):
    __tablename__ = "external_identity"
    id = sa.Column("Id", sa.BigInteger, primary_key=True)
    type = sa.Column("Type", sa.String(16))
    uuid = sa.Column("UUID", sa.String(32))
    external_id = sa.Column("ExternalId", sa.String(32))
    country = sa.Column("Country", sa.String(16))

    TYPE_ARTIST = "artist"
    TYPE_ALBUM = "album"
    TYPE_TRACK = "track"
