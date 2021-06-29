from dotenv.main import find_dotenv
import mysql.connector
import numpy as np
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

host = os.getenv("host")
user = os.getenv("user")
password = os.getenv("password")
database = os.getenv("database")
port = int(os.getenv("port"))

print(port)

mydb = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database,
    port=port
)

mycursor = mydb.cursor()

mycursor.execute('SELECT * FROM datasources limit 10;')

result = mycursor.fetchone()
print(result)
