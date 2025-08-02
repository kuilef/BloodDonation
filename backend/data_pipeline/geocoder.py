import sqlite3
import time
import os
from typing import Dict, Tuple, List, Optional

from geopy.geocoders import GoogleV3
from geopy.extra.rate_limiter import RateLimiter
from unidecode import unidecode

# The Google API key is loaded from an environment variable for security.
# It must be set before running the application.
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if not GOOGLE_API_KEY:
    raise ValueError("GOOGLE_API_KEY environment variable not found.")

geo = GoogleV3(api_key=GOOGLE_API_KEY, timeout=10)

# Rate limiter to avoid burning through API quota
rl = RateLimiter(geo.geocode, min_delay_seconds=0.1, max_retries=2, error_wait_seconds=2.0)

def google_geocode(q: str):
    """Geocodes a query string using GoogleV3, limited to Israel."""
    return rl(q, components={"country": "IL"}, language="iw")

def get_from_cache(cur: sqlite3.Cursor, key: str) -> Optional[Tuple[float, float]]:
    """Retrieves coordinates from the geocache database if the key exists."""
    row = cur.execute("SELECT lat, lon FROM geocache WHERE key=?", (key,)).fetchone()
    return row if row else None

def save_to_cache(cur: sqlite3.Cursor, key: str, lat: float, lon: float, is_exact: bool):
    """Saves or updates a geocoded location in the cache."""
    cur.execute("""
        INSERT OR REPLACE INTO geocache (key, lat, lon, is_exact, updated_at)
        VALUES (?, ?, ?, ?, datetime('now'))
    """, (key, lat, lon, int(is_exact)))

def create_address_key(item: Dict[str, str]) -> str:
    """Creates a normalized, consistent key from address components."""
    city = item.get("City", "").strip()
    street = item.get("Street", "").strip()
    num = item.get("NumHouse", "").strip()
    name = item.get("Name", "").strip()     # IMPORTANT: Name is included to handle cases with only city and name.
    # Using a format that is readable and less prone to separator issues
    return f"{city}, {street}, {num}, {name}".strip().rstrip(',')

def _generate_queries(item: Dict[str, str], use_latin: bool = False) -> List[Tuple[str, bool]]:
    """
    Generates a list of address queries to try, from most specific to least specific.
    This creates a fallback mechanism: if a detailed query fails, a less
    detailed one is tried.
    Returns a list of (query_string, is_exact_flag).
    """
    def tr(s: str) -> str:
        return unidecode(s) if use_latin else s

    city = tr(item.get("City", "").strip())
    street = tr(item.get("Street", "").strip())
    num = tr(item.get("NumHouse", "").strip())
    name = tr(item.get("Name", "").strip())

    queries = []
    
    # --- Query generation logic: from most specific to least specific ---

    # 1. All fields: Name, Street, Number, City
    if name and city and street and num:
        queries.append((f"{name}, {street} {num}, {city}", True))

    # 2. No Name: Street, Number, City
    if city and street and num:
        queries.append((f"{street} {num}, {city}", True))

    # 3. No Number: Name, Street, City
    if name and city and street:
        queries.append((f"{name}, {street}, {city}", True))

    # 4. No Name, No Number: Street, City
    if city and street:
        queries.append((f"{street}, {city}", True))

    # 5. No Street info: Name, City
    if name and city:
        queries.append((f"{name}, {city}", True))

    # 6. Only City (least specific)
    if city:
        queries.append((city, False))
    
    # Remove duplicates while preserving order
    unique_queries = []
    seen = set()
    for q, flag in queries:
        if q not in seen:
            unique_queries.append((q, flag))
            seen.add(q)

    return unique_queries

def get_coordinates(geocache_cursor: sqlite3.Cursor, item: Dict[str, str]) -> Optional[Tuple[float, float]]:
    """
    Main function to get coordinates for a donation item.
    It checks the cache first. On a cache miss, it queries the geocoding API
    and stores the result back in the cache.
    """
    address_key = create_address_key(item)
    
    # 1. Check cache first
    cached_coords = get_from_cache(geocache_cursor, address_key)
    if cached_coords:
        return cached_coords

    # 2. On cache miss, generate queries and try to geocode
    # Try Hebrew first, then fall back to Latin (unidecode)
    all_queries = _generate_queries(item, use_latin=False) + _generate_queries(item, use_latin=True)

    for query, is_exact in all_queries:
        if not query or query.strip() == ',':
            continue
        
        print(f"    [Geocoder] Cache miss for '{address_key}'. Querying Google API with: '{query}'")
        location = google_geocode(query)
        
        if location:
            lat, lon = location.latitude, location.longitude
            # Save to cache for next time
            save_to_cache(geocache_cursor, address_key, lat, lon, is_exact)
            print(f"    [Geocoder] Found and cached: ({lat:.5f}, {lon:.5f})")
            return lat, lon
            
    # 3. If all attempts fail
    return None
