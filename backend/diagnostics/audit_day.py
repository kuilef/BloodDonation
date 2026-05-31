# -*- coding: utf-8 -*-
"""
Диагностика пропавших точек:
- Сопоставляет «сырой» JSON за день с содержимым donations.db
- Проверяет, у каких записей есть/нет координат в geocache.db
- Пытается объяснить причины отсутствия точки на карте
- Находит «слипшиеся» маркеры (одинаковые координаты)
- Выявляет потенциальные конфликты UNIQUE(scheduling_url)

Запуск (из корня проекта):
    python -m backend.diagnostics.audit_day --json 03.09.2025.json --date 2025-09-03

Требования: .env с путями БД уже настроен (как в проекте), БД существуют.
"""

from __future__ import annotations
import argparse
import json
import math
import sqlite3
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, Any, List, Tuple, Optional

# Используем существующие модули проекта и их конфигурации
from backend.db.schema import DONATIONS_DB_PATH, GEOCACHE_DB_PATH
from backend.db.operations import get_donations_by_date, get_db_connection
from backend.data_pipeline.geocoder import create_address_key, _generate_queries  # noqa


def load_raw(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def read_geocache_row(cur: sqlite3.Cursor, key: str) -> Optional[Tuple[float, float, int]]:
    row = cur.execute("SELECT lat, lon, is_exact FROM geocache WHERE key=?", (key,)).fetchone()
    return row if row else None


def norm_coords(lat: float, lon: float, places: int = 5) -> Tuple[float, float]:
    return (round(lat, places), round(lon, places))


def explain_reason(item: Dict[str, Any],
                   in_db: bool,
                   cache_hit: bool,
                   cache_row: Optional[Tuple[float, float, int]],
                   queries: List[Tuple[str, bool]],
                   conn_don: sqlite3.Connection) -> str:
    """Эвристическое объяснение причины отсутствия точки в выдаче."""

    if in_db:
        return "в БД: да"

    # Проверка конфликта уникальности по ссылке (если такая ссылка уже есть, но на другой день)
    url = item.get("SchedulingURL", "").strip()
    if url:
        cur = conn_don.cursor()
        r = cur.execute("SELECT donation_date FROM donations WHERE scheduling_url=?", (url,)).fetchone()
        if r:
            return f"конфликт UNIQUE(scheduling_url), уже в БД на дату {r[0]}"

    city = (item.get("City") or "").strip()
    street = (item.get("Street") or "").strip()
    name = (item.get("Name") or "").strip()
    num = (item.get("NumHouse") or "").strip()

    # Нет записи в кэше координат
    if not cache_hit:
        if not queries:
            return "нет валидных запросов к геокодеру (пустой город/улица/имя)"
        if not city and (not street or not name):
            return "адрес неполный (нет города и недостаёт иных полей) → геокодирование не запускалось/не удалось"
        return "нет координат в кэше → геокодирование не дало результата"

    # Координаты есть в кэше, но в БД точки нет
    if cache_row and not in_db:
        return "координаты в кэше есть, но запись не вставлена (проверить логи пайплайна/исключения)"

    return "неизвестно"


def main():
    ap = argparse.ArgumentParser(description="Аудит соответствия JSON и точек на карте.")
    ap.add_argument("--json", required=True, type=Path, help="Путь к исходному JSON за день")
    ap.add_argument("--date", required=True, help="Дата в формате YYYY-MM-DD для выборки из БД")
    ap.add_argument("--report", type=Path, default=Path("diagnostics_report.md"),
                    help="Куда сохранить markdown-отчёт")
    args = ap.parse_args()

    raw_items = load_raw(args.json)
    total_before = len(raw_items)

    # Фильтруем мультидневной JSON по нужной дате (ищем поля DateDonation или Date)
    def _get_date_str(x):
        return (x.get("DateDonation") or x.get("Date") or "").strip()

    raw_items = [x for x in raw_items if _get_date_str(x).startswith(args.date)]
    print(f"[audit] Загружено сырых записей: {total_before} из {args.json}")
    print(f"[audit] После фильтра по дате {args.date}: {len(raw_items)} записей "
        f"(отброшено {total_before - len(raw_items)})")
    
    # Читаем из БД уже обработанные точки на указанную дату
    processed: List[Dict[str, Any]] = get_donations_by_date(args.date)
    print(f"[audit] В БД на {args.date}: {len(processed)} записей")

    # Индексируем по URL (в схеме он UNIQUE)
    by_url = {r["scheduling_url"]: r for r in processed}

    # Соединения к БД для дополнительных проверок
    conn_don = get_db_connection()
    conn_geo = sqlite3.connect(GEOCACHE_DB_PATH)
    cur_geo = conn_geo.cursor()

    rows_out = []
    reasons_counter = Counter()
    collisions_counter = Counter()
    coords_to_urls = defaultdict(list)

    # Для анализа «слипшихся» маркеров
    for r in processed:
        pair = norm_coords(float(r["latitude"]), float(r["longitude"]))
        collisions_counter[pair] += 1
        coords_to_urls[pair].append(r["scheduling_url"])

    # Основной проход по «сырому» дню
    for idx, item in enumerate(raw_items, 1):
        url = (item.get("SchedulingURL") or "").strip()
        in_db = url in by_url

        key = create_address_key(item)
        cache_row = read_geocache_row(cur_geo, key)
        cache_hit = cache_row is not None

        # Какие запросы пытались бы сгенерироваться для геокодера
        queries = _generate_queries(item, use_latin=False)

        reason = explain_reason(item, in_db, cache_hit, cache_row, queries, conn_don)
        reasons_counter[reason] += 1

        row = {
            "N": idx,
            "Date": (item.get("DateDonation") or "")[:10],
            "City": (item.get("City") or "").strip(),
            "Street": (item.get("Street") or "").strip(),
            "Num": (item.get("NumHouse") or "").strip(),
            "Name": (item.get("Name") or "").strip(),
            "FromTo": f'{item.get("FromHour","")}-{item.get("ToHour","")}',
            "InDB": "yes" if in_db else "no",
            "Cache": "hit" if cache_hit else "miss",
            "Key": key,
            "Reason": reason
        }

        # Добавим краткую информацию о координатах/точности, если в кэше есть
        if cache_row:
            lat, lon, is_exact = cache_row
            row["CacheLat"], row["CacheLon"] = f"{lat:.6f}", f"{lon:.6f}"
            row["CacheExact"] = "exact" if is_exact else "fallback"

        # Для краткости не выводим все запросы, только 1–2 верхних
        if queries:
            top_q = ", ".join(f"‘{q[0]}’" for q in queries[:2])
            row["QueriesTop"] = top_q

        rows_out.append(row)

    conn_geo.close()
    conn_don.close()

    # Сводка коллизий (одинаковые координаты)
    collided = {k: v for k, v in collisions_counter.items() if v > 1}

    # Печать краткой сводки в консоль
    print("\n[audit] Сводка причин отсутствия:")
    for k, v in reasons_counter.most_common():
        print(f"  - {k}: {v}")

    if collided:
        print("\n[audit] Слипшиеся маркеры (одинаковые координаты):")
        for (lat, lon), cnt in collided.items():
            print(f"  - ({lat}, {lon}) ×{cnt}")

    # Сохраним подробный отчёт в Markdown
    md_lines = []
    md_lines.append(f"# Диагностика за {args.date}\n")
    md_lines.append(f"- Сырых записей в JSON: **{len(raw_items)}**")
    md_lines.append(f"- Записей в БД на дату: **{len(processed)}**\n")

    md_lines.append("## Причины отсутствия точек\n")
    for k, v in reasons_counter.most_common():
        md_lines.append(f"- {k}: **{v}**")
    md_lines.append("")

    if collided:
        md_lines.append("## Слипшиеся маркеры\n")
        for (lat, lon), cnt in collided.items():
            urls = ", ".join(coords_to_urls[(lat, lon)][:5])
            md_lines.append(f"- ({lat}, {lon}) ×{cnt} — напр.: {urls}")
        md_lines.append("")

    md_lines.append("## Подробная таблица\n")
    md_lines.append("| # | Date | City | Street | Num | Name | From-To | InDB | Cache | CacheLat | CacheLon | Exact | Reason | QueriesTop | Key |")
    md_lines.append("|---:|:---:|:---|:---|:---:|:---|:---:|:---:|:---:|:---:|:---:|:---:|:---|:---|:---|")
    for r in rows_out:
        md_lines.append(
            "| {N} | {Date} | {City} | {Street} | {Num} | {Name} | {FromTo} | {InDB} | {Cache} | {CacheLat} | {CacheLon} | {CacheExact} | {Reason} | {QueriesTop} | {Key} |"
            .format(
                N=r["N"], Date=r.get("Date",""), City=r.get("City",""), Street=r.get("Street",""),
                Num=r.get("Num",""), Name=r.get("Name",""), FromTo=r.get("FromTo",""),
                InDB=r["InDB"], Cache=r["Cache"],
                CacheLat=r.get("CacheLat",""), CacheLon=r.get("CacheLon",""), CacheExact=r.get("CacheExact",""),
                Reason=r.get("Reason",""), QueriesTop=r.get("QueriesTop",""), Key=r.get("Key","")
            )
        )

    args.report.write_text("\n".join(md_lines), encoding="utf-8")
    print(f"\n[audit] Отчёт сохранён: {args.report.resolve()}")


if __name__ == "__main__":
    main()
