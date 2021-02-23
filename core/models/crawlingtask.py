import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict

from core.models.base_class import Base, TimestampMixin


class Crawlingtask(Base, TimestampMixin):
    __tablename__ = "crawlingtasks"
    id = sa.Column("Id", sa.String(32), primary_key=True)
    actionid = sa.Column("ActionId", sa.String(32), nullable=False)
    objectid = sa.Column("ObjectId", sa.String(32))
    priority = sa.Column("Priority", sa.SmallInteger, nullable=False, default=5)
    taskdetail = sa.Column("TaskDetail", MutableDict.as_mutable(sa.JSON))
    ext = sa.Column("Ext", MutableDict.as_mutable(sa.JSON))
    status = sa.Column("Status", sa.String(32))
