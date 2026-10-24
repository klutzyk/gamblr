# Gamblr

Gamblr is a local-first sports analytics application for NBA and MLB data, statistical projections, model review, and betting-market analysis. It combines a FastAPI backend, PostgreSQL persistence, Python/XGBoost modelling, and a React/Vite dashboard.

The NBA workflow is season-aware and currently resolves to **2026–27**. Explicit historical seasons remain supported, and ingested player rows retain the team they represented in each game so trades do not rewrite history.

> Predictions are probabilistic estimates, not guarantees or financial advice. Small samples, lineup changes, injuries, stale markets, and model drift can materially change an outcome. Never stake money you cannot afford to lose.

## Screenshots

These images were captured from the real local application on 24 October 2026. NBA leaders and matchups came from the live NBA provider; prediction cards came from the checked local model artifacts and restored historical data. The MLB image truthfully shows the supported empty state outside the locally available slate—no odds or predictions were fabricated.

With both development servers running, regenerate the gallery with `Push-Location frontend; npm run screenshots; Pop-Location`.

### 2026–27 NBA leaders

![Gamblr 2026-27 NBA dashboard](docs/screenshots/main-dashboard.png)

### NBA matchup slate

![Gamblr NBA matchup slate](docs/screenshots/nba-matchups.png)

### Player predictions with early-season reliability labels

![Gamblr NBA player predictions](docs/screenshots/player-predictions.png)

### Prediction performance analysis

![Gamblr prediction performance](docs/screenshots/prediction-performance.png)

### MLB analytics and empty-state handling

![Gamblr MLB analytics](docs/screenshots/mlb-analytics.png)

## Features

- NBA schedule, player log, team log, lineup, and per-game box-score ingestion.
- Automatic NBA league-year discovery in US Eastern time plus explicit historical season selection.
- Preseason, regular season, NBA Cup, All-Star, Play-In, and playoff schedule classification.
- Idempotent game ingestion with provider corrections, bounded retries, exponential backoff, timeouts, pacing, and ingestion-run records.
- Points, assists, rebounds, made-three, attempted-three, first-basket, and double/triple-double analysis.
- Sample-aware reliability caps, historical team context, trade detection, and leak-free rolling features.
- Started-game and stale-odds rejection for best-bet candidates.
- MLB prediction, home-run value, and simulation namespaces, with optional data refreshes initiated by the user.
- Operational health diagnostics that separate the database from optional paid integrations.

## Architecture

```mermaid
flowchart LR
    UI[React + Vite] --> API[FastAPI]
    API --> NBA[(Local PostgreSQL\ngamblr)]
    API --> MLB[(Local PostgreSQL\ngamblr_ml)]
    API --> Stats[NBA Stats endpoints]
    API -. optional .-> Odds[Odds / SportsData / PropLine]
    NBA --> Models[Python + XGBoost]
    MLB --> Models
    Models --> API
```

Key locations:

- `backend/app/api/` — HTTP routes for health, NBA, MLB, models, review, odds, and ingestion.
- `backend/app/services/` — provider clients and caching.
- `backend/app/db/` and `backend/app/models/` — persistence logic and SQLAlchemy models.
- `backend/alembic/` — PostgreSQL migrations.
- `ml/nba/` and `ml/mlb/` — feature engineering, training, inference, evaluation, and backtests.
- `frontend/src/` — React dashboard and API client.
- `scripts/` — repeatable Windows setup, migration, startup, ingestion, and pipeline commands.

## Prerequisites

The validated Windows 11 environment used:

- Python 3.13 (the setup script creates `.venv` through `uv`).
- Node.js 24 and npm 11; Node.js 20+ is recommended.
- PostgreSQL 17 locally; PostgreSQL 16+ is supported.
- `uv`, `psql`, and `createdb` on `PATH`.

The application does not need production Supabase credentials for local development. The supplied migration and database-creation scripts reject non-local database hosts.

## Windows setup

From PowerShell in the repository root:

```powershell
.\scripts\setup_windows.ps1
```

This creates `.venv`, installs the pinned Python dependency ranges, installs the frontend from its lockfile, and copies the environment templates when needed.

Review `.env`, then create the two empty local databases. PostgreSQL may prompt for the local `postgres` password:

```powershell
.\scripts\create_local_databases.ps1
.\scripts\migrate.ps1
```

`migrate.ps1` upgrades both `DATABASE_URL` and `ML_DATABASE_URL`, but only when their host is `localhost`, `127.0.0.1`, or `::1`.

Start both development servers:

```powershell
.\scripts\start_dev.ps1
```

Open:

- Frontend: <http://127.0.0.1:5173>
- API documentation: <http://127.0.0.1:8000/docs>
- Dependency health: <http://127.0.0.1:8000/health/dependencies>

The startup script prints both process IDs and the exact `Stop-Process` command for ending the session.

## Environment variables

Copy `.env.example` to `.env`. Never commit `.env` or real API keys.

Required core settings:

- `DATABASE_URL` — async SQLAlchemy URL for the NBA/application database. Local default: `postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/gamblr`.
- `ML_DATABASE_URL` — async URL for the separate MLB/ML database. Local default: `postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/gamblr_ml`.
- `CORS_ORIGINS` — comma-separated browser origins allowed by FastAPI.
- `VITE_API_BASE_URL` — browser-visible backend URL; set the same value in `frontend/.env`.

Optional paid integrations:

- `THEODDS_API_KEY` — NBA bookmaker events and player props.
- `SPORTSDATA_API_KEY` — optional SportsData NBA functions.
- `PROPLINE_API_KEY` — optional MLB prop-line pricing.

Leaving optional keys blank does not block local database, schedule, NBA statistics, model, or frontend development. The affected endpoint returns an actionable “not configured” error, and `/health/dependencies` reports the integration as disabled.

Optional model-window settings are `PRED_HISTORY_WINDOW_DAYS` and `PRED_TEAM_STATS_WINDOW_DAYS`.

## Safely inspecting `backup.dump`

`backup.dump` is a PostgreSQL custom-format archive. Do not restore it over an existing development or production database. Inspect its table of contents first and restore into a new, isolated local database:

```powershell
pg_restore --list .\backup.dump
createdb -h 127.0.0.1 -U postgres gamblr_backup_audit
pg_restore --no-owner --no-privileges -h 127.0.0.1 -U postgres `
  --dbname gamblr_backup_audit .\backup.dump
```

After inspection, choose deliberately whether to keep the audit database or create a fresh `gamblr` database and migrate it. The setup scripts never restore or overwrite a database automatically.

## NBA 2026–27 ingestion

With the API running, ingest a bounded date range:

```powershell
.\scripts\ingest_nba.ps1 `
  -Season 2026-27 `
  -Since 2026-10-22 `
  -Until 2026-10-22
```

The script loads NBA teams, upserts the selected season’s schedule, and ingests player plus team box scores. Omitting `-Season` resolves the league year in `America/New_York`; omitting dates defaults to the current NBA Eastern date.

Equivalent API calls are:

```powershell
Invoke-RestMethod -Method Post `
  'http://127.0.0.1:8000/db/schedule/load?season=2026-27'

Invoke-RestMethod -Method Post `
  'http://127.0.0.1:8000/db/games/ingest?since=2026-10-22&until=2026-10-22&season=2026-27&include_team_stats=true'
```

Re-running the same range is safe. Unique player/game and team/game constraints prevent duplicates, while changed upstream values are reconciled.

For a guided nightly workflow:

```powershell
.\.venv\Scripts\python.exe .\scripts\run_pipeline.py --season 2026-27 --skip-training
```

## Predictions and models

Refresh the leak-free rolling feature cache after ingestion:

```powershell
Invoke-RestMethod -Method Post 'http://127.0.0.1:8000/ml/rolling/update'
```

Retrieve a prediction slate:

```powershell
Invoke-RestMethod `
  'http://127.0.0.1:8000/players/predictions/points?day=today&refresh=true'
```

Model files live under the ignored `ml/models/` directory. If they are unavailable on a new clone, train locally after loading sufficient historical data:

```powershell
Invoke-RestMethod -Method Post 'http://127.0.0.1:8000/ml/train/all'
```

The models use shifted rolling statistics, minutes estimates, team/opponent context, lineup aggregates, teammate context, current-season sample size, and team-change features. Prediction intervals come from the ensemble/calibration artifacts where available.

`confidence` is presented in the UI as **reliability**, not win probability. It is capped at 42% with no current-season games, 48% below three games, 58% below eight, and 68% below fifteen. A detected team change caps it at 55%. Each card also returns `sample_size` and `data_quality`.

Best-bet construction requires an upcoming event and odds updated within 30 minutes by default. It rejects already-started events, future-dated snapshots, and stale markets.

## Tests and checks

Run backend tests from the repository root:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m compileall -q backend ml scripts
```

Run frontend validation:

```powershell
Push-Location frontend
npm run lint
npm run build
npm audit --audit-level=high
Pop-Location
```

The automated suite covers season boundaries, season validation, schedule-phase and V3 box-score normalization, ingestion rerun idempotency, missing/limited prediction data, confidence caps, and odds freshness.

## Troubleshooting

**Database connection failed**

- Confirm PostgreSQL is running and listening on `127.0.0.1:5432`.
- Check both URLs in `.env` and run `scripts\create_local_databases.ps1` followed by `scripts\migrate.ps1`.
- Use `/health/dependencies`; it returns `degraded` and a sanitized database error rather than exposing credentials.

**`psql` or `createdb` is not recognized**

- Add PostgreSQL’s `bin` directory to `PATH`, then open a new PowerShell session.

**NBA provider timeout, 403, or 429**

- Retry a small date range. The client already uses bounded exponential backoff and conservative box-score pacing.
- Do not parallelize bulk box-score calls or bypass upstream access controls.
- Confirm the installed `nba_api` version satisfies `>=1.11.1,<2`.

**No predictions appear**

- Load the season schedule and historical/player game rows.
- Run `/ml/rolling/update`.
- Confirm compatible local artifacts exist under `ml/models/`, or train them locally.
- An empty slate is valid when no games exist for the selected NBA Eastern date.

**Best bets or player props are unavailable**

- Those views require current bookmaker data and `THEODDS_API_KEY`.
- Expired local odds are intentionally excluded.

**MLB shows no current slate**

- This is expected outside an ingested MLB slate. The page displays a stable empty state.
- “Get Predictions” explicitly opts into refreshing source data and computing missing rows; initial page load does not trigger a long external ingestion.

## Development notes

- Production Supabase is intentionally outside the local scripts’ scope.
- Generated models, virtual environments, portable tooling, logs, `.env` files, and secrets are ignored by Git.
- Schema changes belong in Alembic migrations; do not use `Base.metadata.create_all()` as a production migration strategy.
- Preserve game-level `team_abbreviation` when changing roster logic. The mutable `players.team_abbreviation` field represents the latest known team, not historical truth.
