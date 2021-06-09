import os

CORE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(CORE_DIR)
query_path = os.path.join(BASE_DIR, "sources", "query.txt")
credentials_path = os.path.join(BASE_DIR, "sources", "credentials_joy.json")
token_path = os.path.join(BASE_DIR, "sources", "token.pickle")
youtube_com_cookies_path = os.path.join(BASE_DIR, "sources", "youtube_com_cookies.txt")
config_path = os.path.join(CORE_DIR, "mysql_database_connection", "config.json")






