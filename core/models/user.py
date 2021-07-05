import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict

from core.models.base_class import Base, TimestampMixin


class User(Base, TimestampMixin):
    __tablename__ = "Users"
    id = sa.Column("Id", sa.BigInteger, primary_key=True)
    uuid = sa.Column("UUID", sa.String(32), nullable=False, default="")
    valid = sa.Column("Valid", sa.SmallInteger, nullable=False, default=1)
    username = sa.Column("Username", sa.String(128), nullable=False)
    email = sa.Column("Email", sa.String(128))
    password = sa.Column("Password", sa.String(1024))
    blog_url = sa.Column("BlogURL", sa.String(1024))
    blog_title = sa.Column("BlogTitle", sa.String(1024))
    salt = sa.Column("Salt", sa.String(64))
    locale = sa.Column("Locale", sa.String(8), nullable=False, default="en_US")
    auth_service = sa.Column("AuthService", sa.String(32), default=None)
    auth_data = sa.Column("AuthData", sa.String(256), default=None)
    auth_service_access_token = sa.Column(
        "AuthServiceAccessToken", sa.String(3072), default=None
    )
    gender = sa.Column("Gender", sa.String(6), default=None)
    gender_of_interest = sa.Column("GenderOfInterest", sa.String(6))
    profile = sa.Column("Profile", MutableDict.as_mutable(sa.JSON))
    ext = sa.Column("Ext", MutableDict.as_mutable(sa.JSON))
    view_count = sa.Column("ViewCount", sa.BigInteger, default=0)
