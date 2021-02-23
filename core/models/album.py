import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict
from core.models.base_class import Base, TimestampMixin

N_FIRST_ARTIST_PREVIEW = 1


class Album(Base, TimestampMixin):
    __tablename__ = "Albums"
    id = sa.Column("Id", sa.BigInteger, primary_key=True)
    uuid = sa.Column("UUID", sa.String(32), unique=True, nullable=False)
    valid = sa.Column("Valid", sa.SmallInteger, nullable=False, default=1)
    title = sa.Column("Title", sa.String(256), nullable=False)
    release_date = sa.Column("ReleaseDate", sa.DateTime)
    total_tracks = sa.Column("TotalTracks", sa.SmallInteger, nullable=False, default=0)
    itunes_url = sa.Column("iTunesUrl", sa.String(512))
    # noinspection SpellCheckingInspection

    external_id = sa.Column("ItuneAlbumId", sa.Integer, default=None)  # James: Tentatively keep typo in DB.
    ext = sa.Column("Ext", MutableDict.as_mutable(sa.JSON))
    info = sa.Column("Info", MutableDict.as_mutable(sa.JSON))
    artist = sa.Column("Artist", sa.String(256))
    ALBUM_TYPE_FULL = "FULL"
    ALBUM_TYPE_NO_TRACK_HIDDEN = "NO_TRACK_HIDDEN"
    # Convention by data team, make sure that albums has no tracks on data sources will be hidden
    ALBUM_HIDDEN_BY_NO_TRACKS = -91

    SORT_RELEASE_DATE = "RELEASE_DATE"
    SORT_ALPHABETICAL = "ALPHABETICAL"
    SORT_UNIQUE_TRACK = "UNIQUE_TRACK"

    def __str__(self):
        return self.uuid
