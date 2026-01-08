import psycopg
from psycopg.rows import dict_row
from .config import get_db_dsn


def get_conn():
    """Get a PostgreSQL connection."""
    return psycopg.connect(get_db_dsn())


def query_one(sql: str, params: tuple = ()):
    """Execute a query and return the first row as a dictionary."""
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params)
            return cur.fetchone()

