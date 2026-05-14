"""
Database connection pool (psycopg2).

get_db()  — yields a psycopg2 connection from the shared ThreadedConnectionPool.
init_db() — verifies connectivity; schema is managed by Docker init SQL files.

Row access convention throughout the codebase:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM table WHERE id = %s", [id])
        row = cur.fetchone()   # dict | None
"""
import os
from urllib.parse import quote_plus

import psycopg2
import psycopg2.pool
import psycopg2.extras

_pool: psycopg2.pool.ThreadedConnectionPool = None


def _dsn() -> str:
    if os.getenv("DATABASE_URL"):
        return os.getenv("DATABASE_URL")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "postgres")
    user = os.getenv("DB_USER", "postgres")
    pw   = os.getenv("DB_PASSWORD", "")
    if pw:
        return f"postgresql://{user}:{quote_plus(pw)}@{host}:{port}/{name}"
    return f"postgresql://{user}@{host}:{port}/{name}"


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        _pool = psycopg2.pool.ThreadedConnectionPool(minconn=1, maxconn=10, dsn=_dsn())
    return _pool


def get_db():
    """Yield a psycopg2 connection from the pool. Caller controls commit/rollback."""
    pool = _get_pool()
    conn = pool.getconn()
    conn.autocommit = False
    try:
        yield conn
    finally:
        pool.putconn(conn)


def acquire_conn():
    """Acquire a connection from the pool for use in Flask routes."""
    conn = _get_pool().getconn()
    conn.autocommit = False
    return conn


def release_conn(conn):
    """Return a connection to the pool. Always call in a finally block."""
    _get_pool().putconn(conn)


def init_db():
    """Verify DB connectivity. Schema is managed by Docker init SQL files (schema.sql + migrations/)."""
    conn = psycopg2.connect(_dsn())
    conn.close()
    print("Database connection successful.")


if __name__ == "__main__":
    init_db()
