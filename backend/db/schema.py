import sqlite3
import os
import pathlib
import dotenv
dotenv.load_dotenv()

# Define the path to the databases from environment variables
_donations_db_path_str = os.getenv("DONATIONS_DB_PATH")
_geocache_db_path_str = os.getenv("GEOCACHE_DB_PATH")

if not _donations_db_path_str:
    raise ValueError("DONATIONS_DB_PATH environment variable not found.")
if not _geocache_db_path_str:
    raise ValueError("GEOCACHE_DB_PATH environment variable not found.")

# Convert paths to pathlib.Path objects for consistency
DONATIONS_DB_PATH = pathlib.Path(_donations_db_path_str)
GEOCACHE_DB_PATH = pathlib.Path(_geocache_db_path_str)

def create_database():
    """
    Creates the donations.db database and the 'donations' table with necessary indexes.
    This function is idempotent.
    """
    print(f"Initializing donations database at: {DONATIONS_DB_PATH}")
    conn = sqlite3.connect(DONATIONS_DB_PATH)
    cursor = conn.cursor()

    # Create the main donations table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS donations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        donation_date TEXT NOT NULL,
        city TEXT NOT NULL,
        street TEXT,
        num_house TEXT,
        name TEXT NOT NULL,
        from_hour TEXT NOT NULL,
        to_hour TEXT NOT NULL,
        scheduling_url TEXT UNIQUE,
        latitude REAL NOT NULL,
        longitude REAL NOT NULL
    )
    """)

    # Create indexes for faster queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_donation_date ON donations (donation_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_city ON donations (city)")

    conn.commit()
    conn.close()
    print("Donations database schema created successfully.")

def create_geocache_database():
    """
    Creates the geocache.db database and the 'geocache' table.
    The schema is designed for the pipeline's caching geocoder.
    This function is idempotent.
    """
    print(f"Initializing geocache database at: {GEOCACHE_DB_PATH}")
    conn = sqlite3.connect(GEOCACHE_DB_PATH)
    cursor = conn.cursor()

    # key is a normalized string of address components
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS geocache (
        key TEXT PRIMARY KEY,
        lat REAL NOT NULL,
        lon REAL NOT NULL,
        is_exact INTEGER NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    conn.commit()
    conn.close()
    print("Geocache database schema created successfully.")

if __name__ == "__main__":
    create_database()
    create_geocache_database()
