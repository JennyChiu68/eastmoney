#!/usr/bin/env python3
"""Fetch Eastmoney LHB data, with optional watch mode for same-day polling."""

from __future__ import annotations

import argparse
import csv
import json
import ssl
import sys
import time
from collections import defaultdict
from datetime import date, datetime, time as dt_time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import certifi

API_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
PAGE_SIZE = 500
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    )
}
SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())

HS_CODES = {"069001001003", "069001001001"}
KCB_CODES = {"069001001006"}
SZ_CODES = {"069001002001", "069001002005", "069001002002"}
BJ_CODES = {"069001017"}


def parse_args() -> argparse.Namespace:
    today = date.today()
    default_start = date(today.year, 1, 1).isoformat()
    default_end = today.isoformat()

    parser = argparse.ArgumentParser(
        description="Fetch Eastmoney LHB data from a date range."
    )
    parser.add_argument("--start-date", default=default_start, help="YYYY-MM-DD")
    parser.add_argument("--end-date", default=default_end, help="YYYY-MM-DD")
    parser.add_argument(
        "--out-dir",
        default="data/eastmoney_lhb",
        help="Output directory.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.2,
        help="Sleep between page requests.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Retries per page on transient failures.",
    )
    parser.add_argument(
        "--watch-today",
        action="store_true",
        help="Poll for today's data until available, then export and stop.",
    )
    parser.add_argument(
        "--watch-start",
        default="15:30",
        help="Watch start time in HH:MM (for --watch-today).",
    )
    parser.add_argument(
        "--watch-deadline",
        default="20:00",
        help="Watch deadline in HH:MM (for --watch-today).",
    )
    parser.add_argument(
        "--watch-interval-minutes",
        type=float,
        default=5,
        help="Polling interval in minutes (for --watch-today).",
    )
    parser.add_argument(
        "--tz",
        default="Asia/Shanghai",
        help="Timezone for watch mode.",
    )
    parser.add_argument(
        "--daily-subdir",
        default="daily",
        help="Subdirectory under out-dir to store watch-mode daily files.",
    )
    parser.add_argument(
        "--state-subdir",
        default="state",
        help="Subdirectory under out-dir for done markers.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore done marker in watch mode and run anyway.",
    )
    return parser.parse_args()


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(message: str) -> None:
    print(f"[{now_str()}] {message}", flush=True)


def validate_date(date_str: str) -> str:
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"Invalid date format: {date_str} (expected YYYY-MM-DD)") from exc
    return date_str


def parse_hhmm(value: str) -> dt_time:
    try:
        parsed = datetime.strptime(value, "%H:%M")
    except ValueError as exc:
        raise ValueError(f"Invalid HH:MM time: {value}") from exc
    return dt_time(hour=parsed.hour, minute=parsed.minute)


def build_params(start_date: str, end_date: str, page_number: int) -> Dict[str, str]:
    filter_clause = f"(TRADE_DATE>='{start_date}')(TRADE_DATE<='{end_date}')"
    return {
        "reportName": "RPT_DAILYBILLBOARD_DETAILSNEW",
        "columns": (
            "SECURITY_CODE,SECUCODE,SECURITY_NAME_ABBR,TRADE_DATE,CHANGE_RATE,"
            "TRADE_MARKET_CODE,SECURITY_TYPE_CODE,EXPLAIN"
        ),
        "pageNumber": str(page_number),
        "pageSize": str(PAGE_SIZE),
        "sortTypes": "-1,1",
        "sortColumns": "TRADE_DATE,SECURITY_CODE",
        "source": "WEB",
        "client": "WEB",
        "filter": filter_clause,
    }


def fetch_page(
    start_date: str, end_date: str, page_number: int, max_retries: int
) -> Dict[str, Any]:
    params = build_params(start_date, end_date, page_number)
    url = f"{API_URL}?{urlencode(params)}"

    for attempt in range(1, max_retries + 1):
        try:
            req = Request(url, headers=REQUEST_HEADERS, method="GET")
            with urlopen(req, timeout=30, context=SSL_CONTEXT) as resp:
                payload = resp.read().decode("utf-8")
                data = json.loads(payload)
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            if attempt == max_retries:
                raise RuntimeError(
                    f"Request failed on page {page_number} after {max_retries} retries: {exc}"
                ) from exc
            time.sleep(min(2**attempt, 8))
            continue

        if data.get("success") is True:
            return data

        code = data.get("code")
        msg = data.get("message")
        if code == 9201:
            return {"success": True, "result": {"pages": 0, "count": 0, "data": []}}
        if attempt == max_retries:
            raise RuntimeError(
                f"API returned error on page {page_number}: code={code}, message={msg}"
            )
        time.sleep(min(2**attempt, 8))

    raise RuntimeError("Unexpected retry loop exit")


def fetch_all_rows(
    start_date: str, end_date: str, sleep_seconds: float, max_retries: int
) -> List[Dict[str, Any]]:
    first = fetch_page(start_date, end_date, page_number=1, max_retries=max_retries)
    result = first.get("result") or {}
    pages = int(result.get("pages") or 0)
    rows = list(result.get("data") or [])

    for page in range(2, pages + 1):
        time.sleep(sleep_seconds)
        resp = fetch_page(start_date, end_date, page, max_retries=max_retries)
        rows.extend((resp.get("result") or {}).get("data") or [])
    return rows


def normalize_trade_date(value: str) -> str:
    return (value or "").split(" ")[0]


def market_group(trade_market_code: str, security_type_code: str) -> str:
    if security_type_code == "060":
        return "KZZ"
    if trade_market_code in HS_CODES:
        return "HS"
    if trade_market_code in KCB_CODES:
        return "KCB"
    if trade_market_code in SZ_CODES:
        return "SZ"
    if trade_market_code in BJ_CODES:
        return "BJ"
    return "OTHER"


def quote_link(security_code: str, secucode: str) -> str:
    suffix = ""
    if "." in (secucode or ""):
        suffix = secucode.split(".")[1].upper()
    prefix = "1" if suffix == "SH" else "0"
    return f"https://quote.eastmoney.com/unify/r/{prefix}.{security_code}"


def detail_link(security_code: str, trade_date: str) -> str:
    return f"https://data.eastmoney.com/stock/lhb,{trade_date},{security_code}.html"


def to_overview_rows(raw_rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    dedup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in raw_rows:
        trade_date = normalize_trade_date(str(row.get("TRADE_DATE", "")))
        security_code = str(row.get("SECURITY_CODE", ""))
        if not trade_date or not security_code:
            continue
        key = (trade_date, security_code)
        if key in dedup:
            continue
        secucode = str(row.get("SECUCODE", ""))
        dedup[key] = {
            "trade_date": trade_date,
            "security_code": security_code,
            "security_name": row.get("SECURITY_NAME_ABBR", ""),
            "change_rate": row.get("CHANGE_RATE"),
            "trade_market_code": row.get("TRADE_MARKET_CODE", ""),
            "security_type_code": row.get("SECURITY_TYPE_CODE", ""),
            "market_group": market_group(
                str(row.get("TRADE_MARKET_CODE", "")),
                str(row.get("SECURITY_TYPE_CODE", "")),
            ),
            "detail_link": detail_link(security_code, trade_date),
            "quote_link": quote_link(security_code, secucode),
        }

    rows = list(dedup.values())
    group_order = {"HS": 1, "KCB": 2, "SZ": 3, "BJ": 4, "KZZ": 5, "OTHER": 6}
    rows.sort(
        key=lambda x: (
            -int(x["trade_date"].replace("-", "")),
            group_order.get(x["market_group"], 99),
            x["security_code"],
        )
    )
    return rows


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, rows: Iterable[Dict[str, Any]], fieldnames: List[str]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_grouped_json(path: Path, overview_rows: Iterable[Dict[str, Any]]) -> None:
    grouped: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in overview_rows:
        grouped[row["trade_date"]][row["market_group"]].append(row)
    with path.open("w", encoding="utf-8") as f:
        json.dump(grouped, f, ensure_ascii=False, indent=2)


def export_range(
    start_date: str,
    end_date: str,
    out_dir: Path,
    sleep_seconds: float,
    max_retries: int,
    write_when_empty: bool = True,
) -> Dict[str, Any]:
    ensure_dir(out_dir)
    raw_rows = fetch_all_rows(start_date, end_date, sleep_seconds, max_retries)
    overview_rows = to_overview_rows(raw_rows)

    summary: Dict[str, Any] = {
        "start_date": start_date,
        "end_date": end_date,
        "raw_rows": len(raw_rows),
        "overview_rows": len(overview_rows),
        "trade_days": len({r["trade_date"] for r in overview_rows}),
        "raw_path": None,
        "overview_path": None,
        "grouped_path": None,
    }

    if not raw_rows and not write_when_empty:
        return summary

    stamp = f"{start_date.replace('-', '')}_{end_date.replace('-', '')}"
    raw_path = out_dir / f"lhb_raw_{stamp}.csv"
    overview_path = out_dir / f"lhb_overview_{stamp}.csv"
    grouped_path = out_dir / f"lhb_overview_grouped_{stamp}.json"

    raw_fields = [
        "SECURITY_CODE",
        "SECUCODE",
        "SECURITY_NAME_ABBR",
        "TRADE_DATE",
        "CHANGE_RATE",
        "TRADE_MARKET_CODE",
        "SECURITY_TYPE_CODE",
        "EXPLAIN",
    ]
    overview_fields = [
        "trade_date",
        "market_group",
        "security_code",
        "security_name",
        "change_rate",
        "trade_market_code",
        "security_type_code",
        "detail_link",
        "quote_link",
    ]

    write_csv(raw_path, raw_rows, raw_fields)
    write_csv(overview_path, overview_rows, overview_fields)
    write_grouped_json(grouped_path, overview_rows)

    summary["raw_path"] = str(raw_path)
    summary["overview_path"] = str(overview_path)
    summary["grouped_path"] = str(grouped_path)
    return summary


def print_summary(summary: Dict[str, Any]) -> None:
    print(f"Date range: {summary['start_date']} -> {summary['end_date']}")
    print(f"Raw rows: {summary['raw_rows']}")
    print(f"Overview rows (dedup by date+code): {summary['overview_rows']}")
    print(f"Trade days: {summary['trade_days']}")
    if summary.get("raw_path"):
        print(f"Saved raw: {summary['raw_path']}")
        print(f"Saved overview: {summary['overview_path']}")
        print(f"Saved grouped json: {summary['grouped_path']}")
    else:
        print("No files saved (empty result set).")


def mark_done(state_dir: Path, payload: Dict[str, Any]) -> Path:
    ensure_dir(state_dir)
    day = payload["trade_date"]
    done_path = state_dir / f"{day}.done.json"
    with done_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return done_path


def watch_today(args: argparse.Namespace) -> int:
    tz = ZoneInfo(args.tz)
    now_local = datetime.now(tz)
    today = now_local.date().isoformat()

    out_root = Path(args.out_dir).resolve()
    daily_out_dir = out_root / args.daily_subdir
    state_dir = out_root / args.state_subdir
    ensure_dir(daily_out_dir)
    ensure_dir(state_dir)

    done_file = state_dir / f"{today}.done.json"
    if done_file.exists() and not args.force:
        log(f"Done marker exists for {today}, skip. Use --force to rerun. ({done_file})")
        return 0

    start_clock = parse_hhmm(args.watch_start)
    deadline_clock = parse_hhmm(args.watch_deadline)
    start_dt = datetime.combine(now_local.date(), start_clock, tzinfo=tz)
    deadline_dt = datetime.combine(now_local.date(), deadline_clock, tzinfo=tz)
    if deadline_dt <= start_dt:
        raise ValueError("watch-deadline must be later than watch-start")

    if now_local > deadline_dt:
        log(f"Current time passed deadline ({deadline_dt.isoformat()}). Exit.")
        return 0

    if now_local < start_dt:
        wait_seconds = (start_dt - now_local).total_seconds()
        log(
            f"Waiting until watch start {start_dt.isoformat()} "
            f"(sleep {int(wait_seconds)}s)."
        )
        time.sleep(max(wait_seconds, 0))

    interval_seconds = max(1.0, args.watch_interval_minutes * 60.0)
    attempt = 0

    while True:
        now_local = datetime.now(tz)
        if now_local > deadline_dt:
            log(
                f"No update found for {today} before deadline "
                f"{deadline_dt.isoformat()}, stop polling."
            )
            return 0

        attempt += 1
        log(f"Polling attempt {attempt} for {today}.")
        try:
            summary = export_range(
                start_date=today,
                end_date=today,
                out_dir=daily_out_dir,
                sleep_seconds=args.sleep_seconds,
                max_retries=args.max_retries,
                write_when_empty=False,
            )
        except Exception as exc:
            log(f"Attempt {attempt} failed: {exc}")
        else:
            if summary["raw_rows"] > 0:
                done_payload = {
                    "trade_date": today,
                    "captured_at": datetime.now(tz).isoformat(),
                    "watch_start": args.watch_start,
                    "watch_deadline": args.watch_deadline,
                    "watch_interval_minutes": args.watch_interval_minutes,
                    "summary": summary,
                }
                done_path = mark_done(state_dir, done_payload)
                print_summary(summary)
                print(f"Done marker: {done_path}")
                return 0

            log(f"No data yet for {today}.")

        now_local = datetime.now(tz)
        remaining = (deadline_dt - now_local).total_seconds()
        if remaining <= 0:
            continue
        sleep_seconds = min(interval_seconds, remaining)
        log(f"Sleeping {int(sleep_seconds)}s before next poll.")
        time.sleep(sleep_seconds)


def main() -> int:
    args = parse_args()

    if args.watch_today:
        return watch_today(args)

    start_date = validate_date(args.start_date)
    end_date = validate_date(args.end_date)
    if start_date > end_date:
        raise ValueError("start-date must be <= end-date")

    out_dir = Path(args.out_dir).resolve()
    summary = export_range(
        start_date=start_date,
        end_date=end_date,
        out_dir=out_dir,
        sleep_seconds=args.sleep_seconds,
        max_retries=args.max_retries,
        write_when_empty=True,
    )
    print_summary(summary)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
