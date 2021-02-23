import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict

from core.models.base_class import Base, TimestampMixin


class CollectionDataSource(Base):
    __tablename__ = "collection_datasource"
    collection_uuid = sa.Column("CollectionId", sa.String(32), primary_key=True)
    datasource_id = sa.Column("DatasourceId", sa.String(32), primary_key=True)
    priority = sa.Column("Priority", sa.SmallInteger, nullable=False, default=1)
    ext = sa.Column("Ext", MutableDict.as_mutable(sa.JSON))
    is_migrate = sa.Column("IsMigrate", sa.Integer, nullable=False, default=0)
    source_uri = sa.Column("SourceURI", sa.String(255))
    sync = sa.Column("Sync", sa.SmallInteger)
    tag_lines = sa.Column("Taglines", MutableDict.as_mutable(sa.JSON))
    original_id = sa.Column("OriginalId", sa.Integer)

    def __str__(self):
        return f"{self.collection_uuid}-{self.datasource_id}"
