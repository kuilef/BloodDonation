#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
geocode_and_map.py
──────────────────
▪ Принимает JSON-файл в формате, который вы прислали.
▪ Для каждой записи ищет координаты:
      ① Street NumHouse, City
      ② Street, City
      ③ Name, City
      ④ City
      ─ затем те же варианты в латинице (unidecode)
▪ Хранит найденное в SQLite-кэше, чтобы не бить Nominatim повторно.
▪ Делает интерактивную карту (Folium) с цветом маркеров:
      зелёный  – точный адрес
      оранжевый – «≈ по городу» (координата центра города)
▪ Все ненайденные адреса пишутся в missing.csv.
"""

# ───── зависимости ─────────────────────────────────────────────────────────
import json, sys, csv, sqlite3, pathlib, time
from typing import Dict, Tuple, List, Optional

import requests                         # pip install requests
from geopy.geocoders import Nominatim   # pip install geopy
from geopy.extra.rate_limiter import RateLimiter
from unidecode import unidecode         # pip install unidecode
import folium                           # pip install folium


from geopy.geocoders import GoogleV3
from geopy.extra.rate_limiter import RateLimiter

GOOGLE_API_KEY = "AIzaSyCczZzPLT6H7e5LMcJwTYCnnQObjtKA4Sk"           
geo = GoogleV3(api_key=GOOGLE_API_KEY)

# у Google нет явного лимита 1 req/s, но чтобы не сжечь квоту — поставим плавный RateLimiter
rl = RateLimiter(geo.geocode,
                 min_delay_seconds=0.1,           # 10 req/s – безопасно
                 max_retries=2,
                 error_wait_seconds=2.0)

def google_geocode(q: str):
    # components ограничивают поиск страной (IL) и/или районом
    return rl(q, components={"country": "IL"}, language="iw")   # или language="he"

# ───── глобальные константы ────────────────────────────────────────────────
CACHE_DB      = pathlib.Path("geocache.db")
ISRAEL_VIEWBOX = ((29.4, 34.2), (33.5, 35.9))  # ((south, west), (north, east))
NOMI_TPS      = 1                              # ≤ 1 req/s – публичный Nominatim
USER_AGENT    = "blood_donation_map/0.1"

# ───── SQLite кэш ───────────────────────────────────────────────────────────
def init_cache(db: pathlib.Path = CACHE_DB):
    """Инициализирует базу данных кэша с новой схемой."""
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS geocache (
            city       TEXT NOT NULL,
            street     TEXT NOT NULL,
            num_house  TEXT NOT NULL,
            name       TEXT NOT NULL,
            lat        REAL,
            lon        REAL,
            is_exact   INTEGER,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (city, street, num_house, name)
        )
    """)
    conn.commit()
    return conn


def cache_get(cur: sqlite3.Cursor, item: Dict[str, str]) -> Optional[Tuple[float, float, int]]:
    """Ищет запись в кэше по компонентам адреса."""
    query = "SELECT lat, lon, is_exact FROM geocache WHERE city=? AND street=? AND num_house=? AND name=?"
    params = (
        item.get("City", "").strip().lower(),
        item.get("Street", "").strip().lower(),
        item.get("NumHouse", "").strip().lower(),
        item.get("Name", "").strip().lower()
    )
    row = cur.execute(query, params).fetchone()
    return row if row else None


def cache_put(cur: sqlite3.Cursor, item: Dict[str, str], lat: float, lon: float, is_exact: bool):
    """Сохраняет запись в кэш с компонентами адреса."""
    query = """
        INSERT OR REPLACE INTO geocache (city, street, num_house, name, lat, lon, is_exact, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
    """
    params = (
        item.get("City", "").strip().lower(),
        item.get("Street", "").strip().lower(),
        item.get("NumHouse", "").strip().lower(),
        item.get("Name", "").strip().lower(),
        lat,
        lon,
        int(is_exact)
    )
    cur.execute(query, params)


# ───── построение вариантов запроса ────────────────────────────────────────
def _queries(item: Dict[str, str], latin: bool = False) -> List[str]:
    """
    Возвращает список запросов от самого подробного к общему.
    latin=True → все поля прогоняются через unidecode().
    """
    def tr(s: str) -> str:  # transliterate или оставить как есть
        return unidecode(s) if latin else s

    street = tr(item.get("Street", "").strip())
    num = tr(item.get("NumHouse", "").strip())
    city = tr(item.get("City", "").strip())
    name = tr(item.get("Name", "").strip())

    # Строим части адреса, только если они не пустые
    parts = {
        "name": name,
        "street": f"{street} {num}".strip() if street and num else street,
        "city": city,
    }
    
    # Фильтруем пустые части
    valid_parts = {k: v for k, v in parts.items() if v}

    # Генерируем комбинации, сохраняя порядок
    queries = []
    if "name" in valid_parts and "street" in valid_parts and "city" in valid_parts:
        queries.append(f"{valid_parts['name']}, {valid_parts['street']}, {valid_parts['city']}")
    if "street" in valid_parts and "city" in valid_parts:
        queries.append(f"{valid_parts['street']}, {valid_parts['city']}")
    if "name" in valid_parts and "street" in valid_parts: # Редкий случай, но возможный
        queries.append(f"{valid_parts['name']}, {valid_parts['street']}")
    if "name" in valid_parts and "city" in valid_parts:
        queries.append(f"{valid_parts['name']}, {valid_parts['city']}")
    if "city" in valid_parts:
        queries.append(valid_parts['city'])

    # Убираем дубликаты, сохраняя порядок
    return list(dict.fromkeys(queries))


# ───── Nominatim ───────────────────────────────────────────────────────────
# geo = Nominatim(user_agent=USER_AGENT, timeout=10)
# rl  = RateLimiter(geo.geocode, min_delay_seconds=1 / NOMI_TPS,
#                   swallow_exceptions=False)


# def google_geocode(q: str):
#     """Возвращает location или None; уже обёрнуто в RateLimiter."""
#     return rl(q, country_codes="il",
#               viewbox=ISRAEL_VIEWBOX, bounded=True,
#               language="he")


# ───── поиск координат с fallback’ами ──────────────────────────────────────
def find_coords(item: Dict[str, str]) -> Tuple[Optional[Tuple[float, float]], bool]:
    """
    Ищет координаты, возвращая (lat, lon) и флаг точного совпадения.
    Точность определяется на основе location_type ответа Google API.
    ➜ ((lat, lon), is_exact) | (None, False)
    """
    # Получаем все возможные запросы (кириллица + латиница)
    all_queries = _queries(item, latin=False) + _queries(item, latin=True)
    
    # Убираем дубликаты после транслитерации, сохраняя порядок
    unique_queries = list(dict.fromkeys(all_queries))

    for query in unique_queries:
        try:
            loc = google_geocode(query)
            if loc and loc.raw:
                # IMPORTANT: is_exact определяется по ответу API, а не по типу запроса
                location_type = loc.raw.get('geometry', {}).get('location_type', 'APPROXIMATE')
                is_exact = location_type in ['ROOFTOP', 'RANGE_INTERPOLATED']
                
                return (loc.latitude, loc.longitude), is_exact
        except Exception as e:
            # В случае ошибки от API (например, ZERO_RESULTS), просто переходим к следующему запросу
            print(f"      [geocode] Info: query '{query}' failed. Reason: {e}")
            continue
            
    return None, False


# ───── главная функция ─────────────────────────────────────────────────────
def main():
    json_path = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "short.json")
    rows      = json.loads(json_path.read_text(encoding="utf-8"))

    conn = init_cache(); cur = conn.cursor()

    fmap = folium.Map(location=[31.8, 34.8], zoom_start=8)   # центр Израиля
    missing: List[Dict[str, str]] = []
    
    print(f"{'City':<15} {'Street':<20} {'Name':<25} {'Coords':<20} [Source]")

    for it in rows:
        # Ищем в кэше по компонентам адреса
        cached = cache_get(cur, it)
        if cached:
            lat, lon, exact = cached
            source = "cache"
        else:
            # Если не в кэше, ищем через API
            coords, exact = find_coords(it)
            if coords:
                lat, lon = coords
                # Сохраняем в кэш
                cache_put(cur, it, lat, lon, exact)
                source = "google"
            else:
                missing.append(it)
                print(f"✗  NOT FOUND → {it.get('City')} | {it.get('Street')} {it.get('NumHouse')}")
                continue

        # маркер
        color = "green" if exact else "orange"
        folium.Marker(
            [lat, lon],
            icon=folium.Icon(color=color),
            tooltip=f"{it['FromHour']}-{it['ToHour']}",
            popup=folium.Popup(
                f"{it.get('Name','')}, {it.get('Street','')} {it.get('NumHouse','')}, {it['City']}<br>"
                f"<a href='{it['SchedulingURL']}' target='_blank'>Schedule</a><br>"
                f"<i>{source}</i>",
                max_width=400),
        ).add_to(fmap)

        print(f"✓ {it['City']:<15} {it.get('Street',''):<20} {it.get('Name',''):<25} {lat:.5f},{lon:.5f} [{source}]")

    # сохраняем карту и «пропуски»
    html_out = json_path.with_suffix(".html")
    fmap.save(html_out)
    print(f"\nКарта сохранена → {html_out}")

    if missing:
        miss_csv = json_path.with_suffix(".missing.csv")
        with open(miss_csv, "w", newline="", encoding="utf-8") as fp:
            wr = csv.DictWriter(fp, fieldnames=rows[0].keys())
            wr.writeheader(); wr.writerows(missing)
        print(f"⚠️  Не найдено {len(missing)} адресов → {miss_csv}")

    conn.commit(); conn.close()


if __name__ == "__main__":
    main()
