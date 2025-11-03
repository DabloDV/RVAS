from contextlib import contextmanager
from typing import Iterable, Tuple

import psycopg2
from psycopg2.extras import execute_values

from .config import SETTINGS
from .logger import get_logger

logger = get_logger(__name__)


@contextmanager
def get_conn(): #context manager for Postgres connection. Based on SETTINGS.database_url
    conn = psycopg2.connect(SETTINGS.database_url)
    try:
        yield conn
    finally: #on exit
        conn.close() 


def truncate_tables(conn) -> None: #idempotency
    with conn.cursor() as cur:
        logger.info("Truncating tables healthtech.appointments, healthtech.doctors")
        cur.execute("TRUNCATE healthtech.appointments, healthtech.doctors;")


def insert_doctors(conn, rows: Iterable[Tuple[int, str, str]]) -> int:
    sql = """
        INSERT INTO healthtech.doctors (doctor_id, name, specialty)
        VALUES %s
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
        return cur.rowcount


def insert_appointments(conn, rows: Iterable[Tuple[int, int, int, str, str]]) -> int:
    sql = """
        INSERT INTO healthtech.appointments (booking_id, patient_id, doctor_id, booking_date, status)
        VALUES %s
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
        return cur.rowcount