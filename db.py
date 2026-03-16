import os
import sqlite3
from dotenv import load_dotenv

load_dotenv()

DB_MODE = os.getenv("DB_MODE", "sqlite")
POSTGRES_URL = os.getenv("POSTGRES_URL", "")
SQLITE_PATH = os.getenv("SQLITE_PATH", "lead_engine.db")

def get_conn():
    if DB_MODE == "postgres":
        try:
            import psycopg2
        except ImportError as e:
            raise RuntimeError("Postgres mode selected but psycopg2 is not installed") from e
        if not POSTGRES_URL:
            raise RuntimeError("Postgres mode selected but POSTGRES_URL is empty")
        return psycopg2.connect(POSTGRES_URL)

    conn = sqlite3.connect(SQLITE_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

if __name__ == "__main__":
    conn = get_conn()
    print("connected ok")
    conn.close()
