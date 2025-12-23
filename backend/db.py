# db.py
import psycopg2
from psycopg2.extras import RealDictCursor
from config import DB_CONFIG

def get_conn():
    return psycopg2.connect(
        cursor_factory=RealDictCursor,
        **DB_CONFIG
    )
