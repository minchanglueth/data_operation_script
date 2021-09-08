import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict

from core.models.base_class import Base, TimestampMixin


class SgLikes(Base, TimestampMixin):
    __tablename__ = "sg_likes"
    user_uuid = sa.Column("UserUUID", sa.String(32), primary_key=True)
    entity_type = sa.Column("EntityType", sa.String(2), primary_key=True)
    entity_uuid = sa.Column("EntityUUID", sa.String(32), primary_key=True)
    reaction = sa.Column("Reaction", sa.SmallInteger)
    ext = sa.Column("Ext", MutableDict.as_mutable(sa.JSON))

    def __str__(self) -> str:
        return self.user_uuid
