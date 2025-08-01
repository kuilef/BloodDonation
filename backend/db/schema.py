import sqlite3
import pathlib

# Define the path to the databases relative to the backend directory
DB_DIR = pathlib.Path(__file__).parent
DONATIONS_DB_PATH = DB_DIR / "donations.db"

def create_database():
    """
    Creates the donations.db database and the 'donations' table with necessary indexes.
    This function is idempotent.
    """
    print(f"Initializing database at: {DONATIONS_DB_PATH}")
    conn = sqlite3.connect(DONATIONS_DB_PATH)
    cursor = conn.cursor()

    # Create the main donations table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS donations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        donation_date TEXT NOT NULL,
        city TEXT NOT NULL,
        city_english TEXT,
        street TEXT,
        street_english TEXT,
        num_house TEXT,
        name TEXT NOT NULL,
        name_english TEXT,
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
    print("Database schema created successfully.")

if __name__ == "__main__":
    create_database()
