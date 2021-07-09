# from sqlalchemy import create_engine
# import json
# from core import config_path

# import os
# from dotenv import load_dotenv, find_dotenv

# load_dotenv(find_dotenv())

# host = os.getenv("host")
# user = os.getenv("user")
# password = os.getenv("password")
# database = os.getenv("database")
# port = int(os.getenv("port"))
# stg_password = os.getenv("stg_password")
# prod_password = os.getenv("prod_password")


# # config_file = config_path
# # with open(config_file) as json_data_file:
# #     config = json.load(json_data_file)

# if os.getenv("host"):
#     # mysql_config = config['mysql']
#     RDBMS = "mysql"
#     PIP_PACKAGE = "mysqlconnector"
#     SQLALCHEMY_DATABASE_URI = "{}+{}://{}:{}@{}:{}/{}".format(
#         RDBMS, PIP_PACKAGE, user, password, host, port, database
#     )

#     engine = create_engine(SQLALCHEMY_DATABASE_URI)
#     if engine is None:
#         print("failed to connect to MySQL")
#         exit(1)
# else:
#     print("bad config file")
#     exit(1)

from sqlalchemy import create_engine
import json
from core import config_path
from core.mysql_database_connection.mysql_connector import tunnel

tunnel.start()
port = tunnel.local_bind_port

config_file = config_path
with open(config_file) as json_data_file:
    config = json.load(json_data_file)

if config.get('mysql', False):
    mysql_config = config['mysql']
    RDBMS = "mysql"
    PIP_PACKAGE = "mysqlconnector"
    SQLALCHEMY_DATABASE_URI = "{}+{}://{}:{}@{}:{}/{}".format(
        RDBMS, PIP_PACKAGE, mysql_config['user'], mysql_config['password'], #thêm _stg khi test stg
        mysql_config['host'], port, mysql_config['database'])

    engine = create_engine(SQLALCHEMY_DATABASE_URI)
    if engine is None:
        print("failed to connect to MySQL")
        exit(1)
else:
    print("bad config file")
    exit(1)
