import sqlite3
from datetime import date
from typing import List, Dict, Any

import dotenv
dotenv.load_dotenv()
from .schema import DONATIONS_DB_PATH

def _dict_factory(cursor, row):
    """Converts a database row into a dictionary."""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def get_db_connection():
    """Establishes a connection to the SQLite database."""
    conn = sqlite3.connect(DONATIONS_DB_PATH)
    conn.row_factory = _dict_factory
    return conn

def get_donations_by_date(donation_date: str) -> List[Dict[str, Any]]:
    """
    Retrieves all donation records for a specific date.
    The date should be in 'YYYY-MM-DD' format.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM donations WHERE donation_date = ?", (donation_date,))
    donations = cursor.fetchall()
    conn.close()
    return donations

def get_all_cities() -> List[Dict[str, Any]]:
    """
    Retrieves a unique list of all cities with active donation stations.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    # Query for distinct cities, ordering by the Hebrew name
    cursor.execute("""
        SELECT DISTINCT city
        FROM donations
        ORDER BY city
    """)
    cities = cursor.fetchall()
    conn.close()
    return cities
