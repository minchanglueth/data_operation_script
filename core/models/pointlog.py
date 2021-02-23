import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict

from core.models.base_class import Base, TimestampMixin


class PointLog(Base, TimestampMixin):
    __tablename__ = "PointLogs"
    id = sa.Column("Id", sa.String(32), unique=True, nullable=False, primary_key=True)
    user_id = sa.Column("UserId", sa.String(32), unique=True, nullable=False)
    valid = sa.Column("Valid", sa.SmallInteger, nullable=False, default=0)
    info = sa.Column("Info", MutableDict.as_mutable(sa.JSON), default=None, nullable=False)
    verified_info = sa.Column("VerifiedInfo", MutableDict.as_mutable(sa.JSON), default=None, nullable=False)
    action_type = sa.Column("ActionType", sa.String(4))
    target_id = sa.Column("TargetId", sa.String(32), default=None)
    crawler_status = sa.Column("CrawlerStatus", sa.String(16), default=None)
    ext = sa.Column("Ext", MutableDict.as_mutable(sa.JSON))
    ALBUM_NARRATIVE_COMPOSE_ACTION = 'CAN'
    COLLECT_TRACK_ACTION = 'CT'
    COLLECT_YOUTUBE_ACTION = 'CY'
    COLLECT_ARTIST_CONTENT = 'CAC'
    MISSING_TRACK_ALBUM_ACTION = 'MTA'
    MISSING_ALBUM_ARTIST_ACTION = 'MAA'
    MISSING_ARTIST_ACTION = 'MA'


