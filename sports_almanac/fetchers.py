from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from typing import TYPE_CHECKING, Callable, Iterable

import pandas as pd
import requests

from .config import NHL_REQUEST_DELAY_SECONDS, REQUEST_DELAY_SECONDS

if TYPE_CHECKING:
    import nfl_data_py as nfl
    from nba_api.stats.endpoints import LeagueGameFinder

LOGGER = logging.getLogger(__name__)


def _sleep_before_request() -> None:
    time.sleep(REQUEST_DELAY_SECONDS)


def _rate_limited_call(func: Callable[..., object], *args: object, **kwargs: object) -> object:
    _sleep_before_request()
    return func(*args, **kwargs)


def _coerce_numeric(frame: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    for column in columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _monday_floor(series: pd.Series) -> pd.Series:
    return series.dt.normalize() - pd.to_timedelta(series.dt.weekday, unit="D")


def _season_strings(start_date: date, end_date: date) -> list[str]:
    season_starts = set()
    for year in range(start_date.year - 1, end_date.year + 1):
        season_starts.add(year)
    return [f"{year}-{str((year + 1) % 100).zfill(2)}" for year in sorted(season_starts)]


class NFLFetcher:
    def fetch(self, start_date: date, end_date: date) -> pd.DataFrame:
        import nfl_data_py as nfl

        seasons = list(range(start_date.year - 1, end_date.year + 1))
        try:
            pbp = _rate_limited_call(nfl.import_pbp_data, seasons, downcast=True, cache=False)
        except Exception as exc:  # pragma: no cover - external API instability
            LOGGER.warning("NFL fetch failed: %s", exc)
            return pd.DataFrame(columns=["date"])

        if pbp is None or pbp.empty:
            return pd.DataFrame(columns=["date"])

        pbp = pbp.copy()
        date_source = "game_date" if "game_date" in pbp.columns else "gameday"
        pbp["date"] = pd.to_datetime(pbp[date_source], errors="coerce")
        pbp = pbp[(pbp["date"] >= pd.Timestamp(start_date)) & (pbp["date"] <= pd.Timestamp(end_date))]
        if "season_type" in pbp.columns:
            pbp = pbp[pbp["season_type"].astype(str).str.upper().isin(["REG", "REGULAR"])]
        rename_map = {
            "pass_touchdown": "passing_touchdowns",
            "rush_touchdown": "rushing_touchdowns",
            "penalty": "penalties",
            "penalty_yards": "penalty_yards",
            "passing_yards": "passing_yards",
            "rushing_yards": "rushing_yards",
            "fumble": "fumbles",
        }
        pbp = pbp.rename(columns=rename_map)
        metric_columns = list(rename_map.values())
        for column in metric_columns:
            if column not in pbp.columns:
                pbp[column] = 0
        pbp = _coerce_numeric(pbp, metric_columns)
        return pbp.groupby("date", as_index=False)[metric_columns].sum(numeric_only=True)


class NBAFetcher:
    def fetch(self, start_date: date, end_date: date) -> pd.DataFrame:
        from nba_api.stats.endpoints import LeagueGameFinder

        frames: list[pd.DataFrame] = []
        for season in _season_strings(start_date, end_date):
            try:
                endpoint = _rate_limited_call(LeagueGameFinder, league_id_nullable="00", season_nullable=season, timeout=30)
                season_games = endpoint.get_data_frames()[0]
            except Exception as exc:  # pragma: no cover - external API instability
                LOGGER.warning("NBA fetch failed for %s: %s", season, exc)
                continue
            if season_games is None or season_games.empty:
                continue
            frames.append(self._prepare_games(season_games))

        if not frames:
            return pd.DataFrame(columns=["date"])

        daily = pd.concat(frames, ignore_index=True)
        daily = daily[(daily["date"] >= pd.Timestamp(start_date)) & (daily["date"] <= pd.Timestamp(end_date))]
        metric_columns = [column for column in daily.columns if column != "date"]
        return daily.groupby("date", as_index=False)[metric_columns].sum(numeric_only=True)

    def _prepare_games(self, frame: pd.DataFrame) -> pd.DataFrame:
        working = frame.copy()
        working["date"] = pd.to_datetime(working["GAME_DATE"], errors="coerce")
        rename_map = {
            "FG3M": "three_pointers_made",
            "FGA": "field_goal_attempts",
            "FTA": "free_throw_attempts",
            "PF": "personal_fouls",
            "TOV": "turnovers",
            "PTS": "points",
        }
        working = working.rename(columns=rename_map)
        metric_columns = list(rename_map.values())
        for column in metric_columns:
            if column not in working.columns:
                working[column] = 0
        working = _coerce_numeric(working, metric_columns)
        return working[["date", *metric_columns]].dropna(subset=["date"])


class NHLFetcher:
    _SCHEDULE_URL = "https://api-web.nhle.com/v1/schedule/{date}"
    _LANDING_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/landing"
    _STAT_MAP = {
        "sog": "shots_on_goal",
        "pim": "penalty_minutes",
        "hits": "hits_for",
        "blockedShots": "blocked_shots",
    }

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "cross-sport-conspiracy-almanac/1.0"})

    def fetch(self, start_date: date, end_date: date) -> pd.DataFrame:
        games = self._collect_games(start_date, end_date)
        if not games:
            return pd.DataFrame(columns=["date"])

        rows: list[dict] = []
        for game_id, game_date in games:
            try:
                game_rows = self._fetch_game_stats(game_id, game_date)
                rows.extend(game_rows)
            except Exception as exc:  # pragma: no cover - external API instability
                LOGGER.warning("NHL game %s stats fetch failed: %s", game_id, exc)
                continue

        if not rows:
            return pd.DataFrame(columns=["date"])

        daily = pd.DataFrame(rows)
        metric_columns = [col for col in daily.columns if col != "date"]
        daily = _coerce_numeric(daily, metric_columns)
        return daily.groupby("date", as_index=False)[metric_columns].sum(numeric_only=True)

    def _collect_games(self, start_date: date, end_date: date) -> list[tuple[int, date]]:
        """Walk the schedule week-by-week and return (game_id, game_date) pairs for regular-season games."""
        games: list[tuple[int, date]] = []
        current = start_date
        while current <= end_date:
            try:
                time.sleep(NHL_REQUEST_DELAY_SECONDS)
                url = self._SCHEDULE_URL.format(date=current.isoformat())
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:  # pragma: no cover - external API instability
                LOGGER.warning("NHL schedule fetch failed for week of %s: %s", current, exc)
                current += timedelta(days=7)
                continue

            for day in data.get("gameWeek", []):
                day_str = day.get("date", "")
                try:
                    day_date = date.fromisoformat(day_str)
                except (ValueError, TypeError):
                    continue
                if day_date < start_date or day_date > end_date:
                    continue
                for game in day.get("games", []):
                    if game.get("gameType") == 2:
                        game_id = game.get("id")
                        if game_id is not None:
                            games.append((int(game_id), day_date))

            next_start = data.get("nextStartDate")
            if next_start:
                try:
                    next_date = date.fromisoformat(next_start)
                    if next_date <= current:
                        break
                    current = next_date
                except (ValueError, TypeError):
                    current += timedelta(days=7)
            else:
                break

        return games

    def _fetch_game_stats(self, game_id: int, game_date: date) -> list[dict]:
        """Fetch per-team stats from a single game's landing page."""
        time.sleep(NHL_REQUEST_DELAY_SECONDS)
        url = self._LANDING_URL.format(game_id=game_id)
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        away_stats: dict = {"date": pd.Timestamp(game_date)}
        home_stats: dict = {"date": pd.Timestamp(game_date)}

        away_stats["goals"] = float(data.get("awayTeam", {}).get("score", 0))
        home_stats["goals"] = float(data.get("homeTeam", {}).get("score", 0))

        for stat in data.get("summary", {}).get("teamGameStats", []):
            category = stat.get("category", "")
            if category in self._STAT_MAP:
                metric = self._STAT_MAP[category]
                try:
                    away_stats[metric] = float(stat.get("awayValue", 0))
                    home_stats[metric] = float(stat.get("homeValue", 0))
                except (ValueError, TypeError):
                    pass

        return [away_stats, home_stats]


def aggregate_weekly(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["week_start"])
    working = frame.copy()
    working["date"] = pd.to_datetime(working["date"], errors="coerce")
    working = working.dropna(subset=["date"])
    working["week_start"] = _monday_floor(working["date"])
    metric_columns = [column for column in working.columns if column not in {"date", "week_start"}]
    return working.groupby("week_start", as_index=False)[metric_columns].sum(numeric_only=True)
