import sqlite3
import pathlib

# Define the path to the databases relative to the backend directory
DB_DIR = pathlib.Path(__file__).parent
DONATIONS_DB_PATH = DB_DIR / "donations.db"
GEOCACHE_DB_PATH = DB_DIR.parent / "geocache.db"  # In backend/ directory

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
