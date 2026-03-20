import os
from dotenv import load_dotenv

load_dotenv() 

HOST = os.getenv('HOST')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv("DB_PORT")
CLIENT_NUM = 2
DISCORD_ENDPOINT = os.getenv('DISC_ENDPOINT')