import requests
import sqlite3
import pathlib
import json
import re

# Instead of custom geocoder, use unified Google API logic
from ..geocode_and_map import find_coords
from ..db.schema import DONATIONS_DB_PATH

# Define paths relative to this file
PIPELINE_DIR = pathlib.Path(__file__).parent
GEOCACHE_DB_PATH = PIPELINE_DIR.parent / "geocache.db"

MDA_API_URL = "https://www.mdais.org/umbraco/api/invoker/execute"
LANDING_URL = "https://www.mdais.org/blood-donation"


def fetch_mda_data(limit: int | None = 10) -> list:
    """
    Fetches donation station data from the MDA public API.
    First visits the landing page to obtain cookies and CSRF token, then posts to the API.
    Returns only the first `limit` records.
    """
    print("[Processor] Fetching data from MDA API...")

    # 1. Open session and visit landing page to get cookies and CSRF
    session = requests.Session()
    try:
        landing = session.get(LANDING_URL, timeout=10)
        landing.raise_for_status()
    except requests.RequestException as e:
        print(f"[Processor] ERROR: Failed to load landing page: {e}")
        return []

    # 2. Extract CSRF token if present
    csrf_token = None
    match = re.search(r'name="__RequestVerificationToken"\s+value="([^\"]+)"', landing.text)
    if match:
        csrf_token = match.group(1)

    # 3. Prepare headers for API call
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.mdais.org",
        "Referer": LANDING_URL,
        "X-Requested-With": "XMLHttpRequest",
    }
    if csrf_token:
        headers["__RequestVerificationToken"] = csrf_token

    # 4. Prepare payload
    payload = {
        "RequestHeader": {
            "Application": 101,
            "Module": "BloodBank",
            "Function": "GetAllDetailsDonations",
            "Token": ""
        },
        "RequestData": ""
    }

    # 5. Perform API request
    try:
        response = session.post(MDA_API_URL, headers=headers, json=payload, timeout=30)
        print(f"[Processor] API status: {response.status_code}")
        print(f"[Processor] API response body: {response.text[:200]}…")
        response.raise_for_status()

        data = response.json()
        if data and data.get("Result"):
            donations = json.loads(data["Result"])
            if limit is not None:
                donations = donations[:limit]
            print(f"[Processor] Successfully fetched and parsed {len(donations)} records (limit {limit}).")
            return donations
        else:
            print("[Processor] ERROR: 'Result' field missing or empty.")
            return []

    except (requests.RequestException, json.JSONDecodeError) as e:
        print(f"[Processor] ERROR: API request failed: {e}")
        return []


def clear_donations_table(conn: sqlite3.Connection):
    """Deletes all records from the donations table to ensure fresh data."""
    print("[Processor] Clearing old data from donations table...")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM donations")
    conn.commit()
    print("[Processor] Old data cleared.")


def insert_donation(conn: sqlite3.Connection, record: dict):
    """Inserts a single processed donation record into the database."""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO donations (
            donation_date, city, street, num_house, name, from_hour, to_hour,
            scheduling_url, latitude, longitude
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        record['donation_date'], record['city'], record['street'],
        record['num_house'], record['name'], record['from_hour'],
        record['to_hour'], record['scheduling_url'], record['latitude'],
        record['longitude']
    ))


def run_processor():
    """
    Main pipeline:
    - Fetch first 10 records from MDA
    - Use unified Google API (via geocode_and_map.find_coords)
    - Populate database
    """
    print("--- Starting Data Processing Pipeline ---")

    if not GEOCACHE_DB_PATH.exists():
        print(f"[Processor] ERROR: Geocache DB not found at {GEOCACHE_DB_PATH}")
        return

    donations_conn = sqlite3.connect(DONATIONS_DB_PATH)
    geocache_conn = sqlite3.connect(GEOCACHE_DB_PATH)
    geocache_cursor = geocache_conn.cursor()

    mda_stations = fetch_mda_data(limit=100)
    if not mda_stations:
        print("[Processor] No data fetched. Aborting pipeline.")
        return

    clear_donations_table(donations_conn)

    print(f"[Processor] Processing {len(mda_stations)} donation stations...")
    processed_count = 0
    missing_coords_count = 0

    for station in mda_stations:
        coords, exact = find_coords(station)
        if coords:
            lat, lon = coords
            city = station.get('City', '').strip()
            street = station.get('Street', '').strip()
            name = station.get('Name', '').strip()
            record = {
                'donation_date': station['DateDonation'].split('T')[0],
                'city': city,
                'street': street,
                'num_house': station.get('NumHouse', '').strip(),
                'name': name,
                'from_hour': station['FromHour'],
                'to_hour': station['ToHour'],
                'scheduling_url': station['SchedulingURL'],
                'latitude': lat,
                'longitude': lon
            }
            insert_donation(donations_conn, record)
            processed_count += 1
        else:
            missing_coords_count += 1
            print(f"  [Processor] WARNING: Missing coords for {station.get('City')}, {station.get('Street')} ")

    donations_conn.commit()
    geocache_conn.commit()
    donations_conn.close()
    geocache_conn.close()

    print("--- Data Processing Pipeline Finished ---")
    print(f"Processed: {processed_count}, Missing coords: {missing_coords_count}")
