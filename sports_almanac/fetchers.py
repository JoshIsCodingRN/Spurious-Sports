from __future__ import annotations

import logging
import time
from datetime import date
from typing import TYPE_CHECKING, Callable, Iterable

import pandas as pd
import requests

from .config import REQUEST_DELAY_SECONDS

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


def _nhl_season_ids(start_date: date, end_date: date) -> list[int]:
    season_starts = set()
    for year in range(start_date.year - 1, end_date.year + 1):
        season_starts.add(year)
    return [int(f"{year}{year + 1}") for year in sorted(season_starts)]


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
                endpoint = _rate_limited_call(LeagueGameFinder, league_id_nullable="00", season_nullable=season)
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
    base_url = "https://api.nhle.com/stats/rest/en/team/realtime"

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "cross-sport-conspiracy-almanac/1.0"})

    def fetch(self, start_date: date, end_date: date) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for season_id in _nhl_season_ids(start_date, end_date):
            try:
                season_frame = self._fetch_season(season_id)
            except Exception as exc:  # pragma: no cover - external API instability
                LOGGER.warning("NHL fetch failed for %s: %s", season_id, exc)
                continue
            if season_frame.empty:
                continue
            frames.append(season_frame)

        if not frames:
            return pd.DataFrame(columns=["date"])

        daily = pd.concat(frames, ignore_index=True)
        daily = daily[(daily["date"] >= pd.Timestamp(start_date)) & (daily["date"] <= pd.Timestamp(end_date))]
        metric_columns = [column for column in daily.columns if column != "date"]
        return daily.groupby("date", as_index=False)[metric_columns].sum(numeric_only=True)

    def _fetch_season(self, season_id: int) -> pd.DataFrame:
        params = {
            "isAggregate": "false",
            "isGame": "true",
            "start": 0,
            "limit": 3000,
            "sort": '[{"property":"gameDate","direction":"ASC"}]',
            "cayenneExp": f"seasonId={season_id} and gameTypeId=2",
        }
        _sleep_before_request()
        response = self.session.get(self.base_url, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data", [])
        if not data:
            return pd.DataFrame(columns=["date"])
        frame = pd.DataFrame(data)
        if "gameDate" not in frame.columns:
            return pd.DataFrame(columns=["date"])
        frame["date"] = pd.to_datetime(frame["gameDate"], errors="coerce")
        rename_map = {
            "shotsFor": "shots_on_goal",
            "pim": "penalty_minutes",
            "goalsFor": "goals",
            "hitsFor": "hits_for",
            "blockedShotsFor": "blocked_shots",
        }
        frame = frame.rename(columns=rename_map)
        metric_columns = list(rename_map.values())
        for column in metric_columns:
            if column not in frame.columns:
                frame[column] = 0
        frame = _coerce_numeric(frame, metric_columns)
        return frame[["date", *metric_columns]].dropna(subset=["date"])


def aggregate_weekly(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["week_start"])
    working = frame.copy()
    working["date"] = pd.to_datetime(working["date"], errors="coerce")
    working = working.dropna(subset=["date"])
    working["week_start"] = _monday_floor(working["date"])
    metric_columns = [column for column in working.columns if column not in {"date", "week_start"}]
    return working.groupby("week_start", as_index=False)[metric_columns].sum(numeric_only=True)
