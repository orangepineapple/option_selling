import psycopg2
from config.constants import DB_HOST, DB_PORT
import logging

logger = logging.getLogger(__name__)

def connect():
    conn = psycopg2.connect(
        host='192.168.2.60',
        dbname='stocks',
        user='brian',
        password='theorangeman123',
        port = DB_PORT)
    cursor = conn.cursor()
    cursor.execute("SELECT version()")
    logger.info("connection successful")
    return conn


def close_connection(conn):
    '''
    Closes a psycopg2 connection
    '''
    logger.info("closing con")
    conn.cursor().close()
    conn.close()