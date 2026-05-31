# backend/diagnostics/fetch_worker_json.py
# ─────────────────────────────────────────────────────────────────────────────
# Забирает «текущий снимок» JSON с Cloudflare‑Worker (наш прокси MDA)
# и сохраняет его в файл + печатает сводку по количеству точек на даты.
# По умолчанию тянет с https://mda-browser.kuilef42.workers.dev?date=latest
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations
import argparse
import collections
import datetime as dt
import json
import sys
from pathlib import Path

import requests

DEFAULT_URL = "https://mda-browser.kuilef42.workers.dev?date=latest"


def _iso_now_slug() -> str:
    # YYYYMMDDTHHMMSSZ (UTC)
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def main():
    ap = argparse.ArgumentParser(
        description="Fetch MDA snapshot from our Worker and save as JSON"
    )
    ap.add_argument("--url", default=DEFAULT_URL, help="Source URL (Worker)")
    ap.add_argument(
        "--out",
        default=f"mda_snapshot-{_iso_now_slug()}.json",
        help="Output JSON filename",
    )
    ap.add_argument(
        "--stdout",
        action="store_true",
        help="Print JSON to stdout instead of writing a file",
    )
    args = ap.parse_args()

    resp = requests.get(args.url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        print("ERROR: Expected a JSON array from Worker.", file=sys.stderr)
        sys.exit(2)

    # Сводка по датам (DateDonation формата 'YYYY-MM-DDThh:mm:ss')
    by_date = collections.Counter((row.get("DateDonation", "")[:10] for row in data))
    print(f"Fetched {len(data)} records from {args.url}")
    for day, count in sorted(by_date.items()):
        if day:
            print(f"  {day}: {count}")

    if args.stdout:
        json.dump(data, sys.stdout, ensure_ascii=False, indent=2)
        print()
    else:
        out = Path(args.out)
        out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Saved to {out}")


if __name__ == "__main__":
    main()
