# backend/diagnostics/audit_day.py
# ─────────────────────────────────────────────────────────────────────────────
# Сравнение «старого» файла (например, недельной давности) с «текущим» снимком
# по конкретной дате. Показывает, какие точки появились/пропали.
# Может брать «текущий» как из локального файла (--current), так и онлайн
# прямо с Worker (--remote-url, по умолчанию тот же URL).
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations
import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, List

import requests

DEFAULT_URL = "https://mda-browser.kuilef42.workers.dev?date=latest"


def _read_json_file(path: Path) -> List[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"{path} is not a JSON array")
    return data


def _fetch_json(url: str) -> List[dict]:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise ValueError("Remote JSON is not an array")
    return data


def _filter_date(rows: Iterable[dict], day: str) -> List[dict]:
    day = day.strip()
    return [r for r in rows if str(r.get("DateDonation", ""))[:10] == day]


def _key(r: Dict) -> str:
    # Главное — SchedulingURL; если его нет, собираем стабильный ключ‑заменитель
    return r.get("SchedulingURL") or "|".join(
        [
            (r.get("City", "") or "").strip(),
            (r.get("Street", "") or "").strip(),
            (r.get("NumHouse", "") or "").strip(),
            (r.get("Name", "") or "").strip(),
            r.get("FromHour", "") or "",
            r.get("ToHour", "") or "",
        ]
    )


def main():
    ap = argparse.ArgumentParser(description="Audit differences for a given date")
    ap.add_argument("--json", required=True, help="OLD: path to previous multi‑day JSON")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD to audit")

    cur = ap.add_mutually_exclusive_group(required=False)
    cur.add_argument("--current", help="CURRENT: path to current snapshot JSON")
    cur.add_argument("--remote-url", help="Fetch CURRENT live from this URL (Worker)")

    ap.add_argument(
        "--save-remote",
        help="If used with --remote-url, also save fetched JSON to this file",
    )
    ap.add_argument("--show", action="store_true", help="Print missing/extra IDs")
    ap.add_argument(
        "--write-day-json",
        metavar=("OLD_OUT", "CUR_OUT"),
        nargs=2,
        help="Also write filtered‑by‑date JSONs (old/current)",
    )

    args = ap.parse_args()

    old_path = Path(args.json)
    old_all = _read_json_file(old_path)

    # Определяем «текущий» датасет
    if args.current:
        cur_label = f"file:{args.current}"
        cur_all = _read_json_file(Path(args.current))
    else:
        url = args.remote_url or DEFAULT_URL
        cur_label = f"live:{url}"
        cur_all = _fetch_json(url)
        if args.save_remote:
            Path(args.save_remote).write_text(
                json.dumps(cur_all, ensure_ascii=False, indent=2), encoding="utf-8"
            )

    # Фильтрация по нужной дате
    old_day = _filter_date(old_all, args.date)
    cur_day = _filter_date(cur_all, args.date)

    old_keys = { _key(r): r for r in old_day }
    cur_keys = { _key(r): r for r in cur_day }

    missing_now = cur_keys.keys() - old_keys.keys()   # появились теперь, но их не было «тогда»
    disappeared = old_keys.keys() - cur_keys.keys()   # были «тогда», но пропали «сейчас»

    print(
        f"Date {args.date}: old={len(old_day)}; current({cur_label})={len(cur_day)}; Δ={len(cur_day)-len(old_day)}"
    )
    print(f"  present_now_but_missing_in_old: {len(missing_now)}")
    print(f"  present_in_old_but_absent_now:  {len(disappeared)}")

    if args.show:
        if missing_now:
            print("\n— Now present (were missing in OLD):")
            for k in sorted(missing_now):
                item = cur_keys[k]
                print(item.get("SchedulingURL") or k)
        if disappeared:
            print("\n— Disappeared (were present in OLD):")
            for k in sorted(disappeared):
                item = old_keys[k]
                print(item.get("SchedulingURL") or k)

    if args.write_day_json:
        old_out, cur_out = map(Path, args.write_day_json)
        old_out.write_text(
            json.dumps(old_day, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        cur_out.write_text(
            json.dumps(cur_day, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"Saved day JSONs → {old_out} ; {cur_out}")


if __name__ == "__main__":
    main()
