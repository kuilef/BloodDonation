import requests
import sqlite3
import pathlib
import json
import re

# Use the project's canonical, caching geocoder
from .geocoder import get_coordinates
from ..db.schema import DONATIONS_DB_PATH, GEOCACHE_DB_PATH

MDA_API_URL = "https://mda-browser.kuilef42.workers.dev?date=latest"
# LANDING_URL = "https://www.mdais.org/blood-donation"


def fetch_mda_data(limit: int | None = 10) -> list:
    """
    Получает расписание через Cloudflare-Worker.
    Возвращает первые `limit` записей (или все, если limit=None).
    """
    print("[Processor] Fetching data from Worker…")
    try:
        resp = requests.get(MDA_API_URL, timeout=15)
        resp.raise_for_status()
        donations = resp.json()          # Worker отдаёт уже готовый JSON-массив
        if limit is not None:
            donations = donations[:limit]
        print(f"[Processor] Got {len(donations)} records (limit {limit}).")
        return donations
    except (requests.RequestException, json.JSONDecodeError) as e:
        print(f"[Processor] ERROR: fetch failed: {e}")
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
    - Fetch records from MDA
    - Use the caching geocoder to find coordinates
    - Populate database
    """
    print("--- Starting Data Processing Pipeline ---")

    # Database connections are established. The run_pipeline script ensures DBs are created.
    donations_conn = sqlite3.connect(DONATIONS_DB_PATH)
    geocache_conn = sqlite3.connect(GEOCACHE_DB_PATH)
    geocache_cursor = geocache_conn.cursor()

    mda_stations = fetch_mda_data(limit=None)
    if not mda_stations:
        print("[Processor] No data fetched. Aborting pipeline.")
        donations_conn.close()
        geocache_conn.close()
        return

    clear_donations_table(donations_conn)

    print(f"[Processor] Processing {len(mda_stations)} donation stations...")
    processed_count = 0
    missing_coords_count = 0

    for station in mda_stations:
        # This now uses the caching geocoder, passing the database cursor.
        coords = get_coordinates(geocache_cursor, station)
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
