import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base, declared_attr

class CustomBase(object):
    # Default __tablename__
    @declared_attr
    def __tablename__(self):
        return self.__name__.lower()


Base = declarative_base(cls=CustomBase)
BaseMix = declarative_base(cls=CustomBase)


class TimestampMixin:
    created_at = sa.Column("CreatedAt", sa.DateTime, nullable=False, server_default=sa.func.now())
    updated_at = sa.Column("UpdatedAt", sa.DateTime, nullable=False, server_default=sa.func.now(),
                           server_onupdate=sa.func.now())


class PreciseTimestampMixin:
    created_at = sa.Column("CreatedAt", sa.DateTime, nullable=False, server_default=sa.func.now(6))
    updated_at = sa.Column("UpdatedAt", sa.DateTime, nullable=False, server_default=sa.func.now(6),
                           server_onupdate=sa.func.now(6))


