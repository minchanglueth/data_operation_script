import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict

from core.models.base_class import Base, TimestampMixin


class DataSource(Base, TimestampMixin):
    __tablename__ = "DataSources"
    id = sa.Column("Id", sa.String(32), primary_key=True)
    track_id = sa.Column("TrackId", sa.String(32), nullable=False)
    valid = sa.Column("Valid", sa.SmallInteger, nullable=False, default=1)
    format_id = sa.Column("FormatID", sa.String(32), nullable=False)
    source_name = sa.Column("SourceName", sa.String(256))
    source_uri = sa.Column("SourceURI", sa.String(2048))
    is_video = sa.Column("IsVideo", sa.SmallInteger, nullable=False)
    cdn = sa.Column("CDN", sa.String(128), nullable=False, default="")
    file_name = sa.Column("FileName", sa.String(1024))
    artist_name = sa.Column("ArtistName", sa.String(256))
    video_priority = sa.Column("VideoPriority", sa.SmallInteger)
    audio_priority = sa.Column("AudioPriority", sa.SmallInteger)
    duration_ms = sa.Column("DurationMs", sa.Integer)
    width = sa.Column("Width", sa.SmallInteger)
    height = sa.Column("Height", sa.SmallInteger)
    view_count = sa.Column("YoutubeViews", sa.Integer, default=0)
    view_updated_at = sa.Column("YoutubeViewsAt", sa.DateTime)
    info = sa.Column("Info", MutableDict.as_mutable(sa.JSON))
    display_status = sa.Column(
        "DisplayStatus", sa.SmallInteger, nullable=False, default=1
    )
    ext = sa.Column("Ext", MutableDict.as_mutable(sa.JSON))
    integrated_loudness = sa.Column("IntegratedLoudness", sa.Numeric(asdecimal=False))
    old_video_id = sa.Column("OldVideoId", sa.BigInteger)
    youtube_id = sa.Column("youtubeid", sa.String(64), default="")
