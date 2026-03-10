from __future__ import annotations

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

from .config import (
    CORRELATION_THRESHOLD,
    LEAGUE_LABELS,
    MAX_CORRELATIONS,
    METRIC_LABELS,
    MIN_OVERLAP_WEEKS,
    PAGES_DATA_PATH,
)
from .fetchers import NBAFetcher, NFLFetcher, NHLFetcher, aggregate_weekly

LOGGER = logging.getLogger(__name__)


def _humanize_metric(metric: str) -> str:
    return METRIC_LABELS.get(metric, metric.replace("_", " ").title())


def _series_zscores(values: pd.Series) -> np.ndarray:
    z_values = stats.zscore(values.astype(float).to_numpy(), nan_policy="omit")
    if np.isnan(z_values).any() or np.isinf(z_values).any():
        return np.array([])
    return z_values


def _build_points(merged: pd.DataFrame, metric_a: str, metric_b: str, z_a: np.ndarray, z_b: np.ndarray) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for index, row in merged.iterrows():
        week_start = pd.Timestamp(row["week_start"])
        points.append(
            {
                "week_start": week_start.strftime("%Y-%m-%d"),
                "label": f"Week of {week_start.strftime('%Y-%m-%d')}",
                "series_a_raw": round(float(row[metric_a]), 2),
                "series_b_raw": round(float(row[metric_b]), 2),
                "series_a_z": round(float(z_a[index]), 4),
                "series_b_z": round(float(z_b[index]), 4),
            }
        )
    return points


def _correlation_record(
    league_a: str,
    metric_a: str,
    league_b: str,
    metric_b: str,
    merged: pd.DataFrame,
) -> dict[str, Any] | None:
    if len(merged) < MIN_OVERLAP_WEEKS:
        return None
    if merged[metric_a].nunique() < 2 or merged[metric_b].nunique() < 2:
        return None

    z_a = _series_zscores(merged[metric_a])
    z_b = _series_zscores(merged[metric_b])
    if z_a.size == 0 or z_b.size == 0:
        return None

    r_value, p_value = stats.pearsonr(z_a, z_b)
    if np.isnan(r_value) or abs(r_value) < CORRELATION_THRESHOLD:
        return None

    points = _build_points(merged.reset_index(drop=True), metric_a, metric_b, z_a, z_b)
    league_a_label = LEAGUE_LABELS[league_a]
    league_b_label = LEAGUE_LABELS[league_b]
    metric_a_label = _humanize_metric(metric_a)
    metric_b_label = _humanize_metric(metric_b)

    return {
        "id": f"{league_a}_{metric_a}__{league_b}_{metric_b}",
        "headline": f"{league_a_label} {metric_a_label} vs. {league_b_label} {metric_b_label}",
        "summary": f"{league_a_label} {metric_a_label} and {league_b_label} {metric_b_label} moved together with Pearson r = {r_value:.3f} across {len(points)} overlapping weeks.",
        "league_a": league_a_label,
        "metric_a": metric_a,
        "metric_a_label": metric_a_label,
        "league_b": league_b_label,
        "metric_b": metric_b,
        "metric_b_label": metric_b_label,
        "r_score": round(float(r_value), 4),
        "p_value": round(float(p_value), 6),
        "sample_size": len(points),
        "weeks": [point["label"] for point in points],
        "points": points,
    }


def build_correlation_payload(
    start_date: date,
    end_date: date,
    max_results: int = MAX_CORRELATIONS,
    leagues: list[str] | None = None,
) -> dict[str, Any]:
    available_fetchers = {
        "nfl": NFLFetcher(),
        "nba": NBAFetcher(),
        "nhl": NHLFetcher(),
    }

    selected_leagues = leagues or ["nfl", "nba", "nhl"]
    fetchers = {league: available_fetchers[league] for league in selected_leagues if league in available_fetchers}
    weekly_by_league: dict[str, pd.DataFrame] = {}

    for league_key, fetcher in fetchers.items():
        LOGGER.info("Fetching %s data", league_key.upper())
        daily = fetcher.fetch(start_date, end_date)
        weekly_by_league[league_key] = aggregate_weekly(daily)

    correlations: list[dict[str, Any]] = []
    evaluated_pairs = 0
    league_keys = list(weekly_by_league.keys())

    for left_index, league_a in enumerate(league_keys):
        frame_a = weekly_by_league[league_a]
        metric_columns_a = [column for column in frame_a.columns if column != "week_start"]
        if frame_a.empty or not metric_columns_a:
            continue

        for league_b in league_keys[left_index + 1 :]:
            frame_b = weekly_by_league[league_b]
            metric_columns_b = [column for column in frame_b.columns if column != "week_start"]
            if frame_b.empty or not metric_columns_b:
                continue

            for metric_a in metric_columns_a:
                for metric_b in metric_columns_b:
                    merged = (
                        frame_a[["week_start", metric_a]]
                        .merge(frame_b[["week_start", metric_b]], on="week_start", how="inner")
                        .dropna()
                        .sort_values("week_start")
                        .reset_index(drop=True)
                    )
                    evaluated_pairs += 1
                    record = _correlation_record(league_a, metric_a, league_b, metric_b, merged)
                    if record is not None:
                        correlations.append(record)

    correlations.sort(key=lambda item: (abs(item["r_score"]), item["sample_size"]), reverse=True)
    trimmed = correlations[:max_results]
    headline = trimmed[0] if trimmed else None

    return {
        "generated_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "window": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
        "headline_matchup": headline,
        "correlations": trimmed,
        "metadata": {
            "included_leagues": selected_leagues,
            "correlation_threshold": CORRELATION_THRESHOLD,
            "evaluated_pairs": evaluated_pairs,
            "returned_pairs": len(trimmed),
            "minimum_overlap_weeks": MIN_OVERLAP_WEEKS,
        },
    }


def write_payload(payload: dict[str, Any], output_path: Path = PAGES_DATA_PATH) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return output_path
