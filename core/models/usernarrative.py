import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict

from core.models.base_class import Base, TimestampMixin


class UserNarrative(Base, TimestampMixin):
    __tablename__ = "UserNarratives"
    id = sa.Column("Id", sa.String(32), nullable=False, primary_key=True)
    entity_id = sa.Column("EntityId", sa.BigInteger)
    entity_uuid = sa.Column("EntityUUID", sa.String(32))
    entity_type = sa.Column("EntityType", sa.String(2), nullable=False)
    valid = sa.Column("Valid", sa.SmallInteger, nullable=False, default=1)
    importance = sa.Column("Importance", sa.SmallInteger, nullable=False, default=1)
    published_at = sa.Column("PublishedAt", sa.DateTime)
    user_id = sa.Column("UserId", sa.BigInteger, nullable=False)
    title = sa.Column("Title", sa.String(1024), nullable=False)
    content_json = sa.Column(
        "ContentJSON", MutableDict.as_mutable(sa.JSON), nullable=False
    )
    word_count = sa.Column("WordCount", sa.Integer)
    ext = sa.Column("Ext", MutableDict.as_mutable(sa.JSON))
    blog_url = sa.Column("BlogURL", sa.String(1024))
    blog_title = sa.Column("BlogTitle", sa.String(1024))

    SORT_PUBLISHED_AT = "published_at"
