import sqlite3
import time
import os
from typing import Dict, Tuple, List, Optional
import requests

from unidecode import unidecode

# The Google API key is loaded from an environment variable for security.
# It must be set before running the application.
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if not GOOGLE_API_KEY:
    raise ValueError("GOOGLE_API_KEY environment variable not found.")


def google_geocode(q: str) -> Optional[Tuple[float, float]]:
    """Geocodes a query string using Google Places API Text Search.

    This method is preferred for free-form or descriptive queries 
    (e.g., 'near the park entrance') as it can provide more accurate results
    than standard address geocoding.

    Args:
        q: The query string to geocode.

    Returns:
        A tuple of (latitude, longitude) if found, otherwise None.
    """
    params = {
        'query': q,
        'key': GOOGLE_API_KEY,
        'language': 'iw',
        'region': 'IL'  # Bias results to Israel for relevance
    }
    try:
        response = requests.get(
            'https://maps.googleapis.com/maps/api/place/textsearch/json',
            params=params,
            timeout=5  # Set a timeout for network robustness
        )
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        data = response.json()

        if data.get('status') == 'OK' and data.get('results'):
            location = data['results'][0]['geometry']['location']
            return location['lat'], location['lng']
        else:
            # Log non-OK status for debugging, e.g., ZERO_RESULTS
            print(f"    [Geocoder] API returned status: {data.get('status')} for query: '{q}'")
            return None

    except requests.exceptions.RequestException as e:
        print(f"    [Geocoder] HTTP Request failed: {e}")
    except (KeyError, IndexError) as e:
        print(f"    [Geocoder] Failed to parse API response: {e}")
    
    return None

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
        queries.append((f"{city}, {street} {num}, {name}", True))

    # 2. No Name: Street, Number, City
    if city and street and num:
        queries.append((f"{city}, {street} {num}", True))

    # 3. No Number: Name, Street, City
    if name and city and street:
        queries.append((f"{city}, {street}, {name}", True))

    # 4. No Name, No Number: Street, City
    if city and street:
        queries.append((f"{city}, {street}", True))

    # 5. No Street info: Name, City
    if name and city:
        queries.append((f"{city}, {name}", True))

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
        
        # The geocode function now returns a tuple (lat, lon) or None
        coords = google_geocode(query)
        
        if coords:
            lat, lon = coords
            # Save to cache for next time
            save_to_cache(geocache_cursor, address_key, lat, lon, is_exact)
            print(f"    [Geocoder] Found and cached: ({lat:.5f}, {lon:.5f})")
            return lat, lon
        
        # Wait a bit before the next query to avoid hitting API rate limits
        time.sleep(0.1)
            
    # 3. If all attempts fail
    return None
