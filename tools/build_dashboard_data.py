#!/usr/bin/env python3
"""Build static dashboard JSON from Eastmoney overview CSV files."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

MARKET_LABELS = {
    "HS": "沪市",
    "KCB": "科创板",
    "SZ": "深市",
    "BJ": "京市",
    "KZZ": "可转债",
    "OTHER": "其他",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build dashboard JSON.")
    parser.add_argument(
        "--source-dir",
        default="data/eastmoney_lhb",
        help="Source directory containing overview csv files (root + daily).",
    )
    parser.add_argument(
        "--out-dir",
        default="site/data",
        help="Output directory for frontend JSON files.",
    )
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def safe_float(value: Any) -> Optional[float]:
    if value in ("", None):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def infer_date(rows: List[Dict[str, Any]], filename: str) -> Optional[str]:
    if rows and rows[0].get("trade_date"):
        return str(rows[0]["trade_date"])
    m = re.search(r"(\d{8})", filename)
    if not m:
        return None
    raw = m.group(1)
    return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"


def read_csv(path: Path) -> Tuple[Optional[str], List[Dict[str, Any]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    date_str = infer_date(rows, path.name)
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        market_code = str(row.get("market_group", "OTHER"))
        change_rate = safe_float(row.get("change_rate"))
        normalized.append(
            {
                "trade_date": row.get("trade_date") or date_str,
                "security_code": row.get("security_code"),
                "security_name": row.get("security_name"),
                "market_group": market_code,
                "market_label": MARKET_LABELS.get(market_code, "其他"),
                "change_rate": change_rate,
                "trade_market_code": row.get("trade_market_code"),
                "security_type_code": row.get("security_type_code"),
                "detail_link": row.get("detail_link"),
                "quote_link": row.get("quote_link"),
            }
        )
    return date_str, normalized


def read_csv_grouped_by_day(path: Path) -> Dict[str, List[Dict[str, Any]]]:
    _, rows = read_csv(path)
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        date_str = str(row.get("trade_date") or "")
        if not date_str:
            continue
        grouped.setdefault(date_str, []).append(row)
    return grouped


def file_priority(path: Path) -> int:
    # Prefer single-day file over range file when both exist for the same day.
    m = re.search(r"lhb_overview_(\d{8})_(\d{8})\.csv$", path.name)
    if not m:
        return 0
    return 2 if m.group(1) == m.group(2) else 1


def discover_overview_files(source_dir: Path) -> List[Path]:
    root = sorted(source_dir.glob("lhb_overview_*.csv"))
    daily = sorted((source_dir / "daily").glob("lhb_overview_*.csv"))
    return root + daily


def summarize_day(date_str: str, rows: List[Dict[str, Any]], source_file: str) -> Dict[str, Any]:
    market_counts = Counter(row["market_group"] for row in rows)
    changes = [row["change_rate"] for row in rows if row["change_rate"] is not None]
    up_count = sum(1 for v in changes if v > 0)
    down_count = sum(1 for v in changes if v < 0)
    flat_count = sum(1 for v in changes if v == 0)
    avg_change = round(sum(changes) / len(changes), 4) if changes else None

    gainers = sorted(
        [r for r in rows if r["change_rate"] is not None],
        key=lambda x: x["change_rate"],
        reverse=True,
    )[:10]
    losers = sorted(
        [r for r in rows if r["change_rate"] is not None],
        key=lambda x: x["change_rate"],
    )[:10]

    market_breakdown = [
        {
            "code": code,
            "label": MARKET_LABELS.get(code, "其他"),
            "count": count,
        }
        for code, count in sorted(market_counts.items(), key=lambda x: x[0])
    ]

    return {
        "date": date_str,
        "source_file": source_file,
        "summary": {
            "total_count": len(rows),
            "up_count": up_count,
            "down_count": down_count,
            "flat_count": flat_count,
            "avg_change": avg_change,
            "market_counts": dict(market_counts),
        },
        "market_breakdown": market_breakdown,
        "top_gainers": gainers,
        "top_losers": losers,
        "stocks": rows,
    }


def build(source_dir: Path, out_dir: Path) -> Dict[str, Any]:
    ensure_dir(out_dir)
    days_dir = out_dir / "days"
    ensure_dir(days_dir)

    csv_files = discover_overview_files(source_dir)
    per_day_rows: Dict[str, List[Dict[str, Any]]] = {}
    per_day_source: Dict[str, str] = {}
    per_day_priority: Dict[str, int] = {}
    per_day_mtime: Dict[str, float] = {}

    for csv_file in csv_files:
        grouped_rows = read_csv_grouped_by_day(csv_file)
        priority = file_priority(csv_file)
        mtime = csv_file.stat().st_mtime
        for date_str, rows in grouped_rows.items():
            exists = date_str in per_day_rows
            if not exists:
                per_day_rows[date_str] = rows
                per_day_source[date_str] = str(csv_file.relative_to(source_dir))
                per_day_priority[date_str] = priority
                per_day_mtime[date_str] = mtime
                continue

            if priority > per_day_priority[date_str] or (
                priority == per_day_priority[date_str] and mtime > per_day_mtime[date_str]
            ):
                per_day_rows[date_str] = rows
                per_day_source[date_str] = str(csv_file.relative_to(source_dir))
                per_day_priority[date_str] = priority
                per_day_mtime[date_str] = mtime

    dates_desc = sorted(per_day_rows.keys(), reverse=True)
    day_summaries: Dict[str, Dict[str, Any]] = {}
    total_rows = 0
    history_rows: List[Dict[str, Any]] = []

    for date_str in dates_desc:
        rows = per_day_rows[date_str]
        total_rows += len(rows)
        history_rows.extend(rows)
        day_data = summarize_day(date_str, rows, per_day_source[date_str])
        day_summaries[date_str] = day_data
        with (days_dir / f"{date_str}.json").open("w", encoding="utf-8") as f:
            json.dump(day_data, f, ensure_ascii=False, indent=2)

    latest_date = dates_desc[0] if dates_desc else None
    latest_data = day_summaries.get(latest_date) if latest_date else None

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    index = {
        "generated_at_utc": generated_at,
        "total_days": len(dates_desc),
        "total_rows": total_rows,
        "latest_date": latest_date,
        "dates": dates_desc,
    }

    with (out_dir / "index.json").open("w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    with (out_dir / "latest.json").open("w", encoding="utf-8") as f:
        json.dump(latest_data or {}, f, ensure_ascii=False, indent=2)

    history_rows.sort(
        key=lambda x: (
            str(x.get("trade_date", "")),
            str(x.get("security_code", "")),
        ),
        reverse=True,
    )
    with (out_dir / "history.json").open("w", encoding="utf-8") as f:
        json.dump({"rows": history_rows}, f, ensure_ascii=False, indent=2)

    return index


def main() -> int:
    args = parse_args()
    source_dir = Path(args.source_dir).resolve()
    out_dir = Path(args.out_dir).resolve()
    if not source_dir.exists():
        raise FileNotFoundError(f"Source directory not found: {source_dir}")
    result = build(source_dir, out_dir)
    print(
        f"Generated dashboard data: {result['total_days']} day(s), "
        f"{result['total_rows']} row(s), latest={result['latest_date']}"
    )
    print(f"Output dir: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
