from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import func, union, distinct, desc
from core.models.user import User
from core.mysql_database_connection.sqlalchemy_create_engine import SQLALCHEMY_DATABASE_URI
from core.crud.sqlalchemy import get_compiled_raw_mysql

engine = create_engine(SQLALCHEMY_DATABASE_URI)
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))


def get_user_uuid_from_user_name(user_name: list):
    user_uuid_from_username = (db_session.query(User.uuid.label('user_uuid'), User.username.label('user_name'))
                              .select_from(User)
                              .filter(User.valid == 1,
                                      User.username.in_(user_name))
                              )
    return user_uuid_from_username

# if __name__ == "__main__":
#     username = ['Taylor','Cameron']
#     joy = get_compiled_raw_mysql(get_useruuid_from_username(username))
#     print(joy)


