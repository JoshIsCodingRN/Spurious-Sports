# Cross-Sport Conspiracy Almanac

This project generates a static sports dashboard that surfaces strong but logically unrelated weekly correlations across NFL, NBA, and NHL team metrics.

## What The Site Shows

- Weekly aggregated team totals from public or free sports data sources.
- Cross-sport metric pairs normalized with z-scores so unrelated stats can share a chart.
- Pearson correlations filtered to only the strongest matches.
- A static JSON feed in `docs/data/sports_correlations.json` that the frontend reads at page load.

## Current Data Sources

- NFL: `nfl_data_py`
- NBA: `nba_api`
- NHL: NHL public stats API

MLB was removed from the active pipeline because the source path became operationally unreliable due to rate limiting during generation.

## Local Run

Use Python 3.11 for the current dependency set.

```powershell
uv python install 3.11
uv venv --python 3.11 --seed .venv311
uv pip install --python .\.venv311\Scripts\python.exe -r requirements.txt
.\.venv311\Scripts\python.exe generate_correlations.py
```

To reproduce the broader non-empty feed currently checked in:

```powershell
.\.venv311\Scripts\python.exe -c "from datetime import date; from sports_almanac.pipeline import build_correlation_payload, write_payload; write_payload(build_correlation_payload(date(2025, 2, 3), date(2026, 3, 10), 10, ['nfl', 'nba', 'nhl']))"
```

## Frontend

- Static site files live in `docs/`
- Chart rendering is done in `docs/app.js` with Chart.js
- GitHub Pages deploys the `docs/` directory

## GitHub Actions

### Refresh feed

` .github/workflows/update-data.yml ` runs on Tuesdays and can also be triggered manually. It:

- installs Python 3.11
- installs dependencies
- regenerates `docs/data/sports_correlations.json`
- commits the refreshed feed back to `main`

### Deploy Pages

` .github/workflows/deploy-pages.yml ` deploys the static site from `docs/` whenever `main` receives a docs or workflow change.

## Publishing To GitHub

```powershell
git init
git branch -M main
git add .
git commit -m "Initial project scaffold"
git remote add origin <your-repo-url>
git push -u origin main
```

Then in GitHub:

1. Open repository Settings.
2. Open Pages.
3. Set the build source to GitHub Actions.
4. Run the refresh workflow manually once if you want a fresh feed immediately.
