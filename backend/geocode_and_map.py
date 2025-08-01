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

GOOGLE_API_KEY = "AIzaSyCczZzPLT6H7e5LMcJwTYCnnQObjtKA4Sk"             # получите в Google Cloud → APIs & Services → Credentials
geo = GoogleV3(api_key=GOOGLE_API_KEY, timeout=10)

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
    conn = sqlite3.connect(db)
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS geocache (
            key        TEXT PRIMARY KEY,
            lat        REAL,
            lon        REAL,
            is_exact   INTEGER,       -- 1 = дом/улица, 0 = только город
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    return conn


def cache_get(cur, key: str) -> Optional[Tuple[float, float, int]]:
    row = cur.execute("SELECT lat, lon, is_exact FROM geocache WHERE key=?", (key,)).fetchone()
    return row if row else None


def cache_put(cur, key: str, lat: float, lon: float, is_exact: bool):
    cur.execute("""
        INSERT OR REPLACE INTO geocache (key, lat, lon, is_exact, updated_at)
        VALUES (?,?,?,?,datetime('now'))
    """, (key, lat, lon, int(is_exact)))


# ───── построение вариантов запроса ────────────────────────────────────────
def _queries(item: Dict[str, str], latin: bool = False) -> List[Tuple[str, bool]]:
    """
    Возвращает список (query, is_exact). is_exact==False → центр города.
    latin=True  → все поля прогоняются через unidecode().
    """
    def tr(s):  # transliterate или оставить как есть
        return unidecode(s) if latin else s

    street = tr(item.get("Street", "").strip())
    num    = tr(item.get("NumHouse", "").strip())
    city   = tr(item.get("City", "").strip())
    name   = tr(item.get("Name", "").strip())

    out: List[Tuple[str, bool]] = []
    if street and num:
        out.append((f"{street} {num}, {city}", True))
    if street:
        out.append((f"{street}, {city}", True))
    if name:
        out.append((f"{name}, {city}", True))
    if city:
        out.append((city, False))
    return out


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
    ➜ (lat, lon), is_exact
    """
    # ①-④ + ①-④ латиница
    for query, exact in _queries(item, latin=False) + _queries(item, latin=True):
        loc = google_geocode(query)
        if loc:
            return (loc.latitude, loc.longitude), exact
    # Последний шанс – центр города (если не пробовали)
    city = item.get("City", "").strip()
    if city:
        loc = google_geocode(city)
        if loc:
            return (loc.latitude, loc.longitude), False
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
        # ключ кэша – нормализованный адрес
        key = "|".join([
            it.get("City", "").strip().lower(),
            it.get("Street", "").strip().lower(),
            it.get("NumHouse", "").strip().lower(),
            it.get("Name", "").strip().lower()
        ])

        cached = cache_get(cur, key)
        if cached:
            lat, lon, exact = cached
            source = "cache"
        else:
            coords, exact = find_coords(it)
            if coords:
                lat, lon = coords
                cache_put(cur, key, lat, lon, exact)
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
