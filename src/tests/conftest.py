"""Shared pytest fixtures for psycopg2-based integration tests.

Each test gets a real Postgres connection with autocommit=False.
After the test, the connection is always rolled back so no test data
persists in the database.

Requires: docker compose up -d postgres (or a reachable Postgres instance
          configured via DB_* environment variables).
"""
import os
import psycopg2
import psycopg2.extras
import pytest


@pytest.fixture(scope="function")
def db():
    """Yield a psycopg2 connection; always rolls back after the test."""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "postgres"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
    )
    conn.autocommit = False
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()
