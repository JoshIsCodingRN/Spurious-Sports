from __future__ import annotations

import argparse
import logging
from datetime import date
from pathlib import Path

from sports_almanac.config import DateWindow, MAX_CORRELATIONS, PAGES_DATA_PATH
from sports_almanac.pipeline import build_correlation_payload, write_payload

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def parse_args() -> argparse.Namespace:
    window = DateWindow.default()
    parser = argparse.ArgumentParser(description="Generate cross-sport conspiracy correlation JSON.")
    parser.add_argument("--start-date", default=window.start_date.isoformat(), help="Inclusive ISO start date.")
    parser.add_argument("--end-date", default=window.end_date.isoformat(), help="Inclusive ISO end date.")
    parser.add_argument("--max-results", type=int, default=MAX_CORRELATIONS, help="Maximum number of correlations to export.")
    parser.add_argument("--output", default=str(PAGES_DATA_PATH), help="Path to sports_correlations.json.")
    parser.add_argument(
        "--leagues",
        default="nfl,nba,nhl",
        help="Comma-separated league keys to include. Supported: nfl,nba,nhl.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    if start_date > end_date:
        raise ValueError("start-date must be on or before end-date")
    leagues = [league.strip().lower() for league in args.leagues.split(",") if league.strip()]
    payload = build_correlation_payload(
        start_date=start_date,
        end_date=end_date,
        max_results=args.max_results,
        leagues=leagues,
    )
    output_path = PAGES_DATA_PATH if args.output == str(PAGES_DATA_PATH) else Path(args.output)
    destination = write_payload(payload, output_path=output_path)
    logging.info("Wrote %s", destination)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
