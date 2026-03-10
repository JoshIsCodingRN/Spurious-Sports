from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

REQUEST_DELAY_SECONDS = 2
NHL_REQUEST_DELAY_SECONDS = 0.5
CORRELATION_THRESHOLD = 0.70
MIN_OVERLAP_WEEKS = 6
DEFAULT_LOOKBACK_DAYS = 1825
MAX_CORRELATIONS = 10
PAGES_DATA_PATH = Path("docs/data/sports_correlations.json")

LEAGUE_LABELS = {
    "nfl": "NFL",
    "nba": "NBA",
    "nhl": "NHL",
}

METRIC_LABELS = {
    "passing_yards": "Passing Yards",
    "rushing_yards": "Rushing Yards",
    "passing_touchdowns": "Passing Touchdowns",
    "rushing_touchdowns": "Rushing Touchdowns",
    "penalties": "Penalties",
    "penalty_yards": "Penalty Yards",
    "fumbles": "Fumbles",
    "three_pointers_made": "Three-Pointers Made",
    "field_goal_attempts": "Field Goal Attempts",
    "free_throw_attempts": "Free Throw Attempts",
    "personal_fouls": "Personal Fouls",
    "turnovers": "Turnovers",
    "points": "Points",
    "shots_on_goal": "Shots on Goal",
    "penalty_minutes": "Penalty Minutes",
    "goals": "Goals",
    "hits_for": "Hits",
    "blocked_shots": "Blocked Shots",
}


@dataclass(frozen=True)
class DateWindow:
    start_date: date
    end_date: date

    @classmethod
    def default(cls) -> "DateWindow":
        end = date.today()
        start = end - timedelta(days=DEFAULT_LOOKBACK_DAYS)
        return cls(start_date=start, end_date=end)
