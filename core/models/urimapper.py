import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict

from core.models.base_class import Base, TimestampMixin


class URIMapper(Base, TimestampMixin):
    __tablename__ = "urimapper"
    id = sa.Column("Id", sa.String(32), primary_key=True)
    entity_id = sa.Column("EntityId", sa.String(32))
    entity_type = sa.Column("EntityType", sa.String(32))
    valid = sa.Column("Valid", sa.SmallInteger)
    slug = sa.Column("Slug", sa.String(256))
    uri = sa.Column("URI", sa.String(512))
    ext = sa.Column("Ext", MutableDict.as_mutable(sa.JSON))

    def __str__(self) -> str:
        return self.id
