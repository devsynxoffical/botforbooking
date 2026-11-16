#!/usr/bin/env python3
"""
Universal Database Adapter for Cita Previa Booking & Monitor System
Seamlessly supports both PostgreSQL (production / Railway) and SQLite (local dev fallback).
Automatically detects DATABASE_URL or POSTGRES_URL environment variables.
"""

from __future__ import annotations
import os
import re
import sqlite3
import datetime
from pathlib import Path
from typing import Optional, Any, List, Dict, Union

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "orders.db"
MAJOR_SPANISH_PROVINCES = [
    "Madrid", "Barcelona", "Valencia", "Alicante", "Sevilla",
    "Málaga", "Murcia", "Baleares", "Las Palmas", "Santa Cruz de Tenerife",
    "Zaragoza", "Bizkaia", "Tarragona", "Girona", "Granada",
    "A Coruña", "Toledo", "Almería", "Cádiz", "Córdoba",
    "Navarra", "Asturias", "Castellón", "Cantabria", "Valladolid",
    "Gipuzkoa", "Lleida", "Huelva", "Jaén", "Badajoz"
]


def get_database_url() -> Optional[str]:
    raw_url = os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_URL") or os.environ.get("DATABASE_PUBLIC_URL")
    if raw_url and raw_url.strip():
        url = raw_url.strip()
        # Railway provides postgres:// URLs, SQLAlchemy/psycopg2 requires postgresql://
        if url.startswith("postgres://"):
            url = "postgresql://" + url[len("postgres://"):]
        return url
    return None


def is_postgres() -> bool:
    return bool(get_database_url())


# Optional PostgreSQL driver
try:
    import psycopg2
    import psycopg2.extras
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False


def _sql_to_postgres(sql: str) -> str:
    """Translates SQLite style `?` placeholders to PostgreSQL `%s` placeholders."""
    # Replace ? with %s when not inside quotes
    return re.sub(r'\?', '%s', sql)


class PostgresRow(dict):
    """Dict subclass allowing both key lookup, numeric index lookup, and dict conversion."""
    def __init__(self, cursor, row):
        columns = [col.name.lower() for col in cursor.description]
        super().__init__(zip(columns, row))
        self._raw_row = row

    def __getitem__(self, item):
        if isinstance(item, int):
            return self._raw_row[item]
        return super().__getitem__(str(item).lower())

    def get(self, item, default=None):
        return super().get(str(item).lower(), default)


class PostgresCursorWrapper:
    """Wraps psycopg2 cursor to provide sqlite3-compatible interface and parameter translation."""
    def __init__(self, real_cursor):
        self._cur = real_cursor
        self.lastrowid = None

    def execute(self, sql: str, params: Any = None):
        pg_sql = _sql_to_postgres(sql)
        # If INSERT with no RETURNING, append RETURNING id to capture lastrowid
        is_insert = pg_sql.strip().upper().startswith("INSERT INTO")
        has_returning = "RETURNING" in pg_sql.upper()
        if is_insert and not has_returning:
            pg_sql_returning = pg_sql.rstrip(" ;") + " RETURNING id"
            try:
                if params:
                    self._cur.execute(pg_sql_returning, params)
                else:
                    self._cur.execute(pg_sql_returning)
                res = self._cur.fetchone()
                if res:
                    self.lastrowid = res[0]
                return self
            except Exception:
                # Fallback to standard execution if table doesn't have id column
                pass

        if params is not None:
            self._cur.execute(pg_sql, params)
        else:
            self._cur.execute(pg_sql)
        return self

    def executemany(self, sql: str, seq_of_params: Any):
        pg_sql = _sql_to_postgres(sql)
        return self._cur.executemany(pg_sql, seq_of_params)

    def fetchone(self) -> Optional[PostgresRow]:
        row = self._cur.fetchone()
        if row is None:
            return None
        return PostgresRow(self._cur, row)

    def fetchall(self) -> List[PostgresRow]:
        rows = self._cur.fetchall()
        return [PostgresRow(self._cur, r) for r in rows]

    def fetchmany(self, size: int = 1) -> List[PostgresRow]:
        rows = self._cur.fetchmany(size)
        return [PostgresRow(self._cur, r) for r in rows]

    def close(self):
        self._cur.close()

    def __iter__(self):
        for row in self._cur:
            yield PostgresRow(self._cur, row)


class PostgresConnectionWrapper:
    """Wraps psycopg2 connection to mimic sqlite3.Connection with row_factory behavior."""
    def __init__(self, real_conn):
        self._conn = real_conn

    def cursor(self):
        return PostgresCursorWrapper(self._conn.cursor())

    def execute(self, sql: str, params: Any = None):
        cur = self.cursor()
        cur.execute(sql, params)
        return cur

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self._conn.rollback()
        else:
            self._conn.commit()
        self._conn.close()


class DatabaseContextManager:
    """Provides a thread-safe connection context manager for SQLite or PostgreSQL."""
    def __init__(self):
        self.db_url = get_database_url()

    def __enter__(self):
        if self.db_url and HAS_PSYCOPG2:
            try:
                conn = psycopg2.connect(self.db_url)
                return PostgresConnectionWrapper(conn)
            except Exception as e:
                # Log error and fallback to sqlite if connection failed
                print(f"⚠️ PostgreSQL connection failed ({e}), falling back to SQLite.")

        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


def get_db():
    """Returns a connection context manager matching sqlite3 / psycopg2."""
    return DatabaseContextManager()


def init_db():
    """Idempotently initializes schemas, migrations, and province seeds."""
    db_url = get_database_url()
    if db_url and HAS_PSYCOPG2:
        try:
            with get_db() as conn:
                # PostgreSQL schema creation
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS orders (
                        id SERIAL PRIMARY KEY,
                        order_id TEXT UNIQUE NOT NULL,
                        name TEXT NOT NULL,
                        doc_type TEXT NOT NULL,
                        doc_number TEXT NOT NULL,
                        citizenship TEXT NOT NULL,
                        email TEXT NOT NULL,
                        phone TEXT,
                        province TEXT NOT NULL,
                        office TEXT,
                        procedure TEXT NOT NULL,
                        birth_date TEXT,
                        birth_country TEXT,
                        card_expiry_date TEXT,
                        certificate_path TEXT,
                        certificate_password TEXT,
                        status TEXT NOT NULL DEFAULT 'waiting',
                        notes TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                """)
                for col in ["birth_date", "birth_country", "card_expiry_date", "certificate_path", "certificate_password"]:
                    try:
                        conn.execute(f"ALTER TABLE orders ADD COLUMN IF NOT EXISTS {col} TEXT")
                    except Exception:
                        pass

                conn.execute("""
                    CREATE TABLE IF NOT EXISTS monitors (
                        id SERIAL PRIMARY KEY,
                        province TEXT NOT NULL,
                        procedure TEXT NOT NULL,
                        is_active INTEGER DEFAULT 1,
                        interval_seconds INTEGER DEFAULT 60,
                        last_checked TEXT,
                        last_status TEXT DEFAULT 'pending',
                        last_status_message TEXT,
                        last_alert_at TEXT,
                        created_at TEXT NOT NULL
                    )
                """)

                conn.execute("""
                    CREATE TABLE IF NOT EXISTS subscribers (
                        id SERIAL PRIMARY KEY,
                        contact_type TEXT NOT NULL,
                        target TEXT NOT NULL,
                        province TEXT DEFAULT 'ALL',
                        procedure TEXT DEFAULT 'ALL',
                        active INTEGER DEFAULT 1,
                        created_at TEXT NOT NULL
                    )
                """)

                # Seed major Spanish provinces if table is empty
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) as cnt FROM monitors")
                row = cur.fetchone()
                cnt = row["cnt"] if row else 0
                if cnt == 0:
                    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    default_proc = "POLICIA-TOMA DE HUELLA"
                    for prov in MAJOR_SPANISH_PROVINCES:
                        conn.execute(
                            "INSERT INTO monitors (province, procedure, is_active, interval_seconds, created_at) VALUES (?, ?, 1, 60, ?)",
                            (prov, default_proc, now)
                        )
                conn.commit()
            print("✅ PostgreSQL database schema initialized successfully!")
            return
        except Exception as e:
            print(f"⚠️ Error during PostgreSQL init_db ({e}), falling back to SQLite...")

    # SQLite fallback
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                doc_type TEXT NOT NULL,
                doc_number TEXT NOT NULL,
                citizenship TEXT NOT NULL,
                email TEXT NOT NULL,
                phone TEXT NOT NULL,
                province TEXT NOT NULL,
                office TEXT,
                procedure TEXT NOT NULL,
                birth_date TEXT,
                birth_country TEXT,
                card_expiry_date TEXT,
                certificate_path TEXT,
                certificate_password TEXT,
                status TEXT NOT NULL DEFAULT 'waiting',
                notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        for col in ["birth_date", "birth_country", "card_expiry_date", "certificate_path", "certificate_password"]:
            try:
                conn.execute(f"ALTER TABLE orders ADD COLUMN {col} TEXT")
            except sqlite3.OperationalError:
                pass

        conn.execute("""
            CREATE TABLE IF NOT EXISTS monitors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                province TEXT NOT NULL,
                procedure TEXT NOT NULL,
                is_active INTEGER DEFAULT 1,
                interval_seconds INTEGER DEFAULT 60,
                last_checked TEXT,
                last_status TEXT DEFAULT 'pending',
                last_status_message TEXT,
                last_alert_at TEXT,
                created_at TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                contact_type TEXT NOT NULL,
                target TEXT NOT NULL,
                province TEXT DEFAULT 'ALL',
                procedure TEXT DEFAULT 'ALL',
                active INTEGER DEFAULT 1,
                created_at TEXT NOT NULL
            )
        """)

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) as cnt FROM monitors")
        row = cur.fetchone()
        cnt = row["cnt"] if row else 0
        if cnt == 0:
            now = datetime.datetime.now(datetime.timezone.utc).isoformat()
            default_proc = "POLICIA-TOMA DE HUELLA"
            for prov in MAJOR_SPANISH_PROVINCES:
                conn.execute(
                    "INSERT INTO monitors (province, procedure, is_active, interval_seconds, created_at) VALUES (?, ?, 1, 60, ?)",
                    (prov, default_proc, now)
                )
        conn.commit()
    print("✅ SQLite database schema initialized successfully!")
