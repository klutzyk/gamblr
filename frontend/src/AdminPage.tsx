import { useEffect, useMemo, useState } from "react";
import {
  evaluateAllPredictions,
  getLatestIngestionRun,
  getMlbCronStatus,
  getRecentMlbPlayerGameRows,
  getRecentPlayerGameDates,
  recalcUnderRiskAll,
  refreshPlayerTeamAbbr,
  runWalkforwardBacktest,
  startGamesIngestJob,
  startLastNUpdateJob,
  startMlbBootstrapIngest,
  startMlbPredictionPrecompute,
  startTrainAllMlb,
  startTrainMlbMarket,
  trainAllModels,
  updateRollingFeatures,
  updateTeamGames,
  getUpdateJobStatus,
  getMlbHrEvBoard,
  type MlbRecentPlayerGameRow,
  type MlbCronStatusResponse,
  type MlbMarketName,
} from "./api";
import "./AdminPage.css";

type BacktestStat = "points" | "assists" | "rebounds" | "threept" | "threepa";
type AdminSportTab = "nba" | "mlb";

const MLB_MARKETS: MlbMarketName[] = [
  "batter_home_runs",
  "batter_hits",
  "batter_total_bases",
  "pitcher_strikeouts",
];

const BACKTEST_STATS: BacktestStat[] = [
  "points",
  "assists",
  "rebounds",
  "threept",
  "threepa",
];

const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const toLocalIsoDate = (date = new Date()) => {
  const localTime = date.getTime() - date.getTimezoneOffset() * 60_000;
  return new Date(localTime).toISOString().slice(0, 10);
};

const formatElapsed = (elapsedMs: number) => {
  const totalSeconds = Math.max(0, Math.floor(elapsedMs / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes <= 0) return `${seconds}s`;
  return `${minutes}m ${seconds}s`;
};

export default function AdminPage() {
  const todayIso = toLocalIsoDate();
  const [activeSportTab, setActiveSportTab] = useState<AdminSportTab>("nba");
  const [sinceDate, setSinceDate] = useState("");
  const [untilDate, setUntilDate] = useState(todayIso);
  const [season, setSeason] = useState("2025-26");
  const [useGameIngest, setUseGameIngest] = useState(true);
  const [updateTeamGamesAfterIngest, setUpdateTeamGamesAfterIngest] = useState(false);
  const [refreshPlayerTeams, setRefreshPlayerTeams] = useState(true);
  const [fallbackRefresh, setFallbackRefresh] = useState(true);
  const [updateActuals, setUpdateActuals] = useState(true);
  const [updateRolling, setUpdateRolling] = useState(true);
  const [recalcUnderRisk, setRecalcUnderRisk] = useState(true);
  const [trainModels, setTrainModels] = useState(false);
  const [runBacktests, setRunBacktests] = useState(false);
  const [selectedBacktests, setSelectedBacktests] = useState<BacktestStat[]>([
    "assists",
    "rebounds",
    "threept",
    "threepa",
  ]);
  const [isRunning, setIsRunning] = useState(false);
  const [logs, setLogs] = useState<string[]>([]);
  const [latestIngestionText, setLatestIngestionText] = useState("Loading...");
  const [progressLabel, setProgressLabel] = useState<string | null>(null);
  const [progressPct, setProgressPct] = useState<number | null>(null);
  const [latestGameDates, setLatestGameDates] = useState<
    Array<{
      id: number;
      player_id: number;
      game_id: string;
      game_date: string;
      matchup: string | null;
      points: number | null;
      assists: number | null;
      rebounds: number | null;
    }>
  >([]);
  const [mlbSeason, setMlbSeason] = useState("2026");
  const [mlbDate, setMlbDate] = useState(todayIso);
  const [mlbPrecomputeLimit, setMlbPrecomputeLimit] = useState(200);
  const [mlbTrainMarket, setMlbTrainMarket] = useState<MlbMarketName>("batter_home_runs");
  const [mlbTrainAll, setMlbTrainAll] = useState(false);
  const [mlbMinPlayerGames, setMlbMinPlayerGames] = useState(3);
  const [mlbSearchModels, setMlbSearchModels] = useState(false);
  const [mlbStatus, setMlbStatus] = useState<MlbCronStatusResponse | null>(null);
  const [mlbStatusLoading, setMlbStatusLoading] = useState(false);
  const [latestMlbPlayerGames, setLatestMlbPlayerGames] = useState<MlbRecentPlayerGameRow[]>([]);

  const canRun = useMemo(() => {
    if (isRunning) return false;
    if (!runBacktests) return true;
    return selectedBacktests.length > 0;
  }, [isRunning, runBacktests, selectedBacktests.length]);

  const appendLog = (line: string) => {
    const stamp = new Date().toLocaleTimeString();
    setLogs((prev) => [...prev, `[${stamp}] ${line}`]);
  };

  const runWithHeartbeat = async <T,>(
    label: string,
    task: () => Promise<T>,
    heartbeatMs = 30000
  ) => {
    const startedAt = Date.now();
    appendLog(`${label} started...`);
    const interval = window.setInterval(() => {
      const elapsed = formatElapsed(Date.now() - startedAt);
      appendLog(`${label} still running... (${elapsed} elapsed)`);
    }, heartbeatMs);

    try {
      const result = await task();
      appendLog(`${label} finished in ${formatElapsed(Date.now() - startedAt)}.`);
      return result;
    } finally {
      window.clearInterval(interval);
    }
  };

  const pollJob = async (jobId: string, label: string) => {
    appendLog(`${label} job queued: ${jobId}`);
    while (true) {
      const status = await getUpdateJobStatus(jobId);
      const done = status.steps_done ?? status.games_done ?? status.players_done ?? 0;
      const total = status.steps_total ?? status.games_total ?? status.players_total ?? null;
      const current =
        status.current_market ?? status.current_day ?? status.current_date ?? status.current_game_date ?? status.current_game_id;
      const pct =
        typeof total === "number" && total > 0
          ? Math.max(0, Math.min(100, (Number(done) / total) * 100))
          : null;
      setProgressPct(pct);
      setProgressLabel(
        total && total > 0
          ? `${label}: ${done}/${total}${current ? ` - ${current}` : ""}`
          : `${label}: ${status.status}${current ? ` - ${current}` : ""}`
      );
      appendLog(
        total && total > 0
          ? `${label} ${status.status} (${done}/${total})${current ? ` - ${current}` : ""}`
          : `${label} ${status.status}${current ? ` - ${current}` : ""}`
      );
      if (status.status === "completed") {
        setProgressPct(100);
        setProgressLabel(`${label} completed.`);
        appendLog(`${label} result: ${JSON.stringify(status.result ?? status)}`);
        return status;
      }
      if (status.status === "failed") {
        throw new Error(`${label} failed: ${status.error ?? "unknown error"}`);
      }
      if (status.status === "not_found") {
        throw new Error(`${label} job not found: ${jobId}`);
      }
      await wait(8000);
    }
  };

  const toggleBacktest = (stat: BacktestStat) => {
    setSelectedBacktests((prev) =>
      prev.includes(stat) ? prev.filter((s) => s !== stat) : [...prev, stat]
    );
  };

  const runPipeline = async () => {
    setIsRunning(true);
    setLogs([]);
    setProgressLabel(null);
    setProgressPct(null);
    try {
      if (sinceDate) {
        if (useGameIngest) {
          appendLog(`Starting per-game ingest job from ${sinceDate}...`);
          const started = await startGamesIngestJob({
            since: sinceDate,
            until: untilDate || undefined,
            season,
            include_team_stats: true,
          });
          appendLog(`Game ingest job queued: ${started.job_id}`);
          while (true) {
            const status = await getUpdateJobStatus(started.job_id);
            const done = status.games_done ?? 0;
            const total = status.games_total ?? null;
            const pct =
              total && total > 0 ? Math.max(0, Math.min(100, (done / total) * 100)) : null;
            setProgressPct(pct);
            setProgressLabel(
              total && total > 0
                ? `Ingesting games: ${done}/${total}${
                    status.current_game_date ? ` • ${status.current_game_date}` : ""
                  }${status.current_game_id ? ` • ${status.current_game_id}` : ""}`
                : "Preparing game ingest..."
            );
            appendLog(
              total && total > 0
                ? `Game ingest ${status.status} (${done}/${total})`
                : `Game ingest ${status.status}`
            );
            if (status.status === "completed") {
              const ingestResult = status.result ?? {};
              appendLog(`Game ingest done: ${JSON.stringify(ingestResult)}`);
              setProgressPct(100);
              setProgressLabel("Game ingest completed.");
              break;
            }
            if (status.status === "failed") {
              throw new Error(`Game ingest failed: ${status.error ?? "unknown error"}`);
            }
            await wait(8000);
          }
          if (updateTeamGamesAfterIngest) {
            const teamResult = await runWithHeartbeat("Updating team game logs", () =>
              updateTeamGames(season)
            );
            appendLog(`Team games update done: ${JSON.stringify(teamResult)}`);
          }
        } else {
          appendLog(`Starting per-player ingest job from ${sinceDate}...`);
          const started = await startLastNUpdateJob({
            since: sinceDate,
            until: untilDate || undefined,
            season,
          });
          appendLog(`Job queued: ${started.job_id}`);
          while (true) {
            const status = await getUpdateJobStatus(started.job_id);
            const done = status.players_done ?? 0;
            const total = status.players_total ?? "n/a";
            setProgressPct(
              typeof status.players_total === "number" && status.players_total > 0
                ? Math.max(0, Math.min(100, (done / status.players_total) * 100))
                : null
            );
            setProgressLabel(`Ingesting players: ${done}/${total}`);
            appendLog(`Job ${status.status} (${done}/${total})`);
            if (status.status === "completed") {
              setProgressPct(100);
              setProgressLabel("Per-player ingest completed.");
              break;
            }
            if (status.status === "failed") {
              throw new Error(`Ingest job failed: ${status.error ?? "unknown error"}`);
            }
            await wait(8000);
          }
          appendLog("Per-player ingest completed.");
          if (updateTeamGamesAfterIngest) {
            const teamResult = await runWithHeartbeat("Updating team game logs", () =>
              updateTeamGames(season)
            );
            appendLog(`Team games update done: ${JSON.stringify(teamResult)}`);
          }
        }
      } else {
        appendLog("Ingest skipped (no since date provided).");
      }

      if (refreshPlayerTeams) {
        const refreshResult = await runWithHeartbeat(
          "Refreshing player team abbreviations",
          () =>
            refreshPlayerTeamAbbr({
              season,
              fallback: fallbackRefresh,
            })
        );
        appendLog(`Refresh done: ${JSON.stringify(refreshResult)}`);
      }

      if (updateActuals) {
        const actualsResult = await runWithHeartbeat("Updating prediction actuals", () =>
          evaluateAllPredictions()
        );
        appendLog(`Actuals update done: ${JSON.stringify(actualsResult)}`);
      }

      if (updateRolling) {
        const rollingResult = await runWithHeartbeat("Updating rolling features", () =>
          updateRollingFeatures()
        );
        appendLog(`Rolling update done: ${JSON.stringify(rollingResult)}`);
      }

      if (recalcUnderRisk) {
        const underRiskResult = await runWithHeartbeat(
          "Recalculating under-risk metrics",
          () => recalcUnderRiskAll()
        );
        appendLog(`Under-risk recalc done: ${JSON.stringify(underRiskResult)}`);
      }

      if (trainModels) {
        const trainResult = await runWithHeartbeat("Training models", () =>
          trainAllModels()
        );
        appendLog(`Training done: ${JSON.stringify(trainResult)}`);
      } else {
        appendLog("Model training skipped.");
      }

      if (runBacktests) {
        const stats = selectedBacktests.slice();
        appendLog(`Running walk-forward backtests: ${stats.join(", ")}`);
        for (const stat of stats) {
          const backtestResult = await runWithHeartbeat(
            `${stat} walk-forward backtest`,
            () => runWalkforwardBacktest(stat, { reset: false })
          );
          appendLog(`${stat} backtest done: ${JSON.stringify(backtestResult)}`);
        }
      } else {
        appendLog("Backtests skipped.");
      }

      appendLog("Pipeline finished successfully.");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      appendLog(`Pipeline failed: ${message}`);
      setProgressLabel(`Failed: ${message}`);
    } finally {
      setIsRunning(false);
    }
  };

  const runAdminAction = async (label: string, action: () => Promise<void>) => {
    setIsRunning(true);
    setProgressLabel(null);
    setProgressPct(null);
    try {
      appendLog(`${label} started...`);
      await action();
      appendLog(`${label} finished.`);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      appendLog(`${label} failed: ${message}`);
      setProgressLabel(`Failed: ${message}`);
    } finally {
      setIsRunning(false);
    }
  };

  const refreshMlbStatus = async () => {
    setMlbStatusLoading(true);
    try {
      const status = await getMlbCronStatus(mlbDate);
      setMlbStatus(status);
      appendLog(`MLB status ${status.date}: ${JSON.stringify(status.prediction_market_counts ?? {})}`);
    } catch (error) {
      appendLog(`MLB status failed: ${error instanceof Error ? error.message : "Unknown error"}`);
    } finally {
      setMlbStatusLoading(false);
    }
  };

  const loadRecentMlbRows = async () => {
    try {
      const res = await getRecentMlbPlayerGameRows(5);
      setLatestMlbPlayerGames(res.data ?? []);
    } catch {
      setLatestMlbPlayerGames([]);
    }
  };

  const runMlbScheduleRoster = () =>
    runAdminAction("MLB slate data ingest", async () => {
      const started = await startMlbBootstrapIngest({
        season: Number(mlbSeason),
        since: mlbDate,
        until: mlbDate,
        final_only: false,
        include_savant: false,
        include_weather: true,
        include_umpire_roster: true,
      });
      await pollJob(started.job_id, "MLB slate data ingest");
      await refreshMlbStatus();
      await loadRecentMlbRows();
    });

  const runMlbPrecomputeStart = () =>
    runAdminAction("MLB prediction precompute", async () => {
      const started = await startMlbPredictionPrecompute({
        days: mlbDate,
        limit_per_market: mlbPrecomputeLimit,
        refresh: true,
        ensure_data: false,
      });
      await pollJob(started.job_id, "MLB prediction precompute");
      await refreshMlbStatus();
    });

  const runMlbTrain = () =>
    runAdminAction("MLB model training", async () => {
      const started = mlbTrainAll
        ? await startTrainAllMlb({
            min_player_games: mlbMinPlayerGames,
            search_models: mlbSearchModels,
          })
        : await startTrainMlbMarket(mlbTrainMarket, {
            min_player_games: mlbMinPlayerGames,
            search_models: mlbSearchModels,
          });
      await pollJob(started.job_id, "MLB model training");
    });

  const runMlbHrOddsRefresh = () =>
    runAdminAction("MLB HR odds refresh", async () => {
      const result = await runWithHeartbeat("Refreshing MLB HR odds/value board", () =>
        getMlbHrEvBoard({
          date: mlbDate,
          day: "auto",
          bookmaker: "fanduel",
          max_events: 30,
          max_age_minutes: 30,
          refresh: true,
          prediction_limit: 300,
          limit: 75,
          refresh_key: Date.now(),
        })
      );
      appendLog(`HR odds/value: matched ${result.matched}, positive EV ${result.positive_ev.length}`);
      await refreshMlbStatus();
    });

  useEffect(() => {
    const loadLatestIngestion = async () => {
      try {
        const res = await getLatestIngestionRun();
        if (!res.data) {
          const latestGameDate = res.latest_game_date
            ? String(res.latest_game_date).slice(0, 10)
            : "n/a";
          setLatestIngestionText(
            `No previous ingestion runs found. Latest player_game_stats date: ${latestGameDate}`
          );
          return;
        }
        const createdAt = new Date(res.data.created_at).toLocaleString();
        const since = res.data.since_date ?? "n/a";
        const latestGameDate = res.latest_game_date
          ? String(res.latest_game_date).slice(0, 10)
          : "n/a";
        setLatestIngestionText(
          `Last run: ${createdAt} | since: ${since} | season: ${res.data.season ?? "n/a"} | status: ${res.data.status} | latest game_date: ${latestGameDate}`
        );
      } catch {
        setLatestIngestionText("Could not load last ingestion run.");
      }
    };
    const loadRecentGameDates = async () => {
      try {
        const res = await getRecentPlayerGameDates(5);
        setLatestGameDates(res.data ?? []);
      } catch {
        setLatestGameDates([]);
      }
    };
    void loadLatestIngestion();
    void loadRecentGameDates();
    void refreshMlbStatus();
    void loadRecentMlbRows();
  }, []);

  return (
    <div className="admin-shell">
      <div className="container py-5">
        <div className="admin-card admin-card-narrow">
          <div className="admin-header">
            <h2 className="mb-1">Data Ingestion Console</h2>
            <p className="mb-0">NBA pipeline controls and MLB operations from one panel.</p>
          </div>

          <div className="admin-tabs mt-4">
            <button
              type="button"
              className={`admin-tab ${activeSportTab === "nba" ? "active" : ""}`}
              onClick={() => setActiveSportTab("nba")}
            >
              NBA
            </button>
            <button
              type="button"
              className={`admin-tab ${activeSportTab === "mlb" ? "active" : ""}`}
              onClick={() => setActiveSportTab("mlb")}
            >
              MLB
            </button>
          </div>

          {activeSportTab === "nba" && (
          <>
          <div className="admin-section-title mt-4">
            <span>NBA</span>
            <strong>Ingestion and model maintenance</strong>
          </div>

          <div className="row g-3">
            <div className="col-md-3">
              <label className="admin-label">Ingest Since (YYYY-MM-DD)</label>
              <input
                className="admin-input"
                type="date"
                value={sinceDate}
                onChange={(e) => setSinceDate(e.target.value)}
              />
              <small className="admin-help">Leave empty to skip ingestion.</small>
            </div>
            <div className="col-md-2">
              <label className="admin-label">Until Date</label>
              <input
                className="admin-input"
                type="date"
                value={untilDate}
                onChange={(e) => setUntilDate(e.target.value)}
              />
            </div>
            <div className="col-md-2">
              <label className="admin-label">Season</label>
              <input
                className="admin-input"
                value={season}
                onChange={(e) => setSeason(e.target.value)}
              />
            </div>
            <div className="col-md-2">
              <label className="admin-label">Ingest Mode</label>
              <select
                className="admin-select"
                value={useGameIngest ? "game" : "player"}
                onChange={(e) => setUseGameIngest(e.target.value === "game")}
              >
                <option value="game">Per-game (faster)</option>
                <option value="player">Per-player job</option>
              </select>
            </div>
          </div>

          <div className="admin-options mt-4">
            <label><input type="checkbox" checked={updateTeamGamesAfterIngest} onChange={(e) => setUpdateTeamGamesAfterIngest(e.target.checked)} /> Update team game logs after ingest</label>
            <label><input type="checkbox" checked={refreshPlayerTeams} onChange={(e) => setRefreshPlayerTeams(e.target.checked)} /> Refresh active player team abbreviations</label>
            <label><input type="checkbox" checked={fallbackRefresh} onChange={(e) => setFallbackRefresh(e.target.checked)} /> Allow per-player fallback on refresh</label>
            <label><input type="checkbox" checked={updateActuals} onChange={(e) => setUpdateActuals(e.target.checked)} /> Update prediction actuals</label>
            <label><input type="checkbox" checked={updateRolling} onChange={(e) => setUpdateRolling(e.target.checked)} /> Update rolling features</label>
            <label><input type="checkbox" checked={recalcUnderRisk} onChange={(e) => setRecalcUnderRisk(e.target.checked)} /> Recalculate under-risk metrics</label>
            <label><input type="checkbox" checked={trainModels} onChange={(e) => setTrainModels(e.target.checked)} /> Train models</label>
            <label><input type="checkbox" checked={runBacktests} onChange={(e) => setRunBacktests(e.target.checked)} /> Run walk-forward backtests</label>
          </div>

          {runBacktests && (
            <div className="admin-backtests mt-3">
              {BACKTEST_STATS.map((stat) => (
                <label key={stat} className="me-3">
                  <input
                    type="checkbox"
                    checked={selectedBacktests.includes(stat)}
                    onChange={() => toggleBacktest(stat)}
                  />{" "}
                  {stat}
                </label>
              ))}
            </div>
          )}

          <div className="mt-4 d-flex gap-2 align-items-center">
            <button className="btn btn-success" onClick={runPipeline} disabled={!canRun}>
              {isRunning ? "Running..." : "Run Pipeline"}
            </button>
          </div>
          </>
          )}

          {activeSportTab === "mlb" && (
          <>

          <div className="admin-section-title mt-4">
            <span>MLB</span>
            <strong>Slate data, predictions, odds, and models</strong>
          </div>

          <div className="row g-3 mt-2">
            <div className="col-md-3">
              <label className="admin-label">Slate Date (MLB / US)</label>
              <input
                className="admin-input"
                type="date"
                value={mlbDate}
                onChange={(e) => setMlbDate(e.target.value)}
              />
              <small className="admin-help">
                MLB official date, usually one day behind Australia for games already played today.
              </small>
            </div>
            <div className="col-md-2">
              <label className="admin-label">Season</label>
              <input
                className="admin-input"
                value={mlbSeason}
                onChange={(e) => setMlbSeason(e.target.value)}
              />
            </div>
            <div className="col-md-2">
              <label className="admin-label">Limit / Market</label>
              <input
                className="admin-input"
                type="number"
                min={1}
                max={500}
                value={mlbPrecomputeLimit}
                onChange={(e) => setMlbPrecomputeLimit(Number(e.target.value) || 200)}
              />
            </div>
            <div className="col-md-5 d-flex align-items-end gap-2 flex-wrap">
              <button className="btn btn-outline-dark btn-sm" onClick={() => void refreshMlbStatus()} disabled={mlbStatusLoading}>
                {mlbStatusLoading ? "Checking..." : "Check MLB Status"}
              </button>
            </div>
          </div>

          <div className="admin-action-grid mt-3">
            <div className="admin-action-panel">
              <h6>Slate Data</h6>
              <p className="admin-help mb-3">
                Runs the one-date MLB ingest for schedule, rosters, game feeds, weather, and umpires.
              </p>
              <button className="btn btn-sm btn-success" onClick={runMlbScheduleRoster} disabled={isRunning}>
                Ingest Slate Data
              </button>
            </div>

            <div className="admin-action-panel">
              <h6>Predictions</h6>
              <p className="admin-help mb-3">
                Runs the stored prediction precompute for the selected slate date with refresh enabled.
              </p>
              <button className="btn btn-sm btn-success" onClick={runMlbPrecomputeStart} disabled={isRunning}>
                Precompute Slate
              </button>
            </div>

            <div className="admin-action-panel">
              <h6>Home Run Value</h6>
              <p className="admin-help mb-3">Fetch FanDuel HR odds and join to stored/model HR predictions for the selected date.</p>
              <button className="btn btn-sm btn-success" onClick={runMlbHrOddsRefresh} disabled={isRunning}>
                Refresh HR Odds
              </button>
            </div>

            <div className="admin-action-panel">
              <h6>Training</h6>
              <label className="admin-label">Market</label>
              <select className="admin-select" value={mlbTrainMarket} onChange={(e) => setMlbTrainMarket(e.target.value as MlbMarketName)} disabled={mlbTrainAll}>
                {MLB_MARKETS.map((market) => <option key={market} value={market}>{market}</option>)}
              </select>
              <div className="admin-options admin-options-compact mt-2">
                <label><input type="checkbox" checked={mlbTrainAll} onChange={(e) => setMlbTrainAll(e.target.checked)} /> Train all</label>
                <label><input type="checkbox" checked={mlbSearchModels} onChange={(e) => setMlbSearchModels(e.target.checked)} /> Search models</label>
              </div>
              <label className="admin-label mt-2">Min Player Games</label>
              <input className="admin-input" type="number" min={0} max={100} value={mlbMinPlayerGames} onChange={(e) => setMlbMinPlayerGames(Number(e.target.value) || 0)} />
              <button className="btn btn-sm btn-success mt-3" onClick={runMlbTrain} disabled={isRunning}>
                Start Training
              </button>
            </div>
          </div>
          </>
          )}

          <div className="admin-logs mt-4">
            {(isRunning || progressLabel) && (
              <div className="admin-progress-wrap">
                <div className="admin-progress-meta">
                  <span>{progressLabel ?? "Running..."}</span>
                  <span>{progressPct !== null ? `${progressPct.toFixed(0)}%` : "..."}</span>
                </div>
                <div className="admin-progress-bar">
                  <div
                    className={`admin-progress-fill${progressPct === null ? " indeterminate" : ""}`}
                    style={progressPct !== null ? { width: `${progressPct}%` } : undefined}
                  ></div>
                </div>
              </div>
            )}
            <div className="admin-log-meta">
              {activeSportTab === "mlb"
                ? `MLB slate ${mlbStatus?.date ?? mlbDate} | games: ${mlbStatus?.games_count ?? "-"} | rosters: ${mlbStatus?.roster_rows ?? "-"} | predictions: ${mlbStatus?.predictions_complete ? "ready" : "missing"} | HR odds: ${mlbStatus?.hr_odds_rows ?? "-"}`
                : latestIngestionText}
            </div>
            {logs.length === 0 ? (
              <p className="mb-0 text-muted">No runs yet.</p>
            ) : (
              logs.map((line, idx) => (
                <div key={`${line}-${idx}`} className="admin-log-line">
                  {line}
                </div>
              ))
            )}
          </div>

          {activeSportTab === "nba" ? (
            <div className="admin-recent mt-3">
              <h6 className="mb-2">Latest 5 rows in `player_game_stats`</h6>
              {latestGameDates.length === 0 ? (
                <p className="mb-0 text-muted">No recent rows available.</p>
              ) : (
                <div className="table-responsive">
                  <table className="table table-sm mb-0">
                    <thead>
                      <tr>
                        <th>ID</th>
                        <th>Game Date</th>
                        <th>Player</th>
                        <th>Game</th>
                        <th>Matchup</th>
                        <th>PTS</th>
                        <th>AST</th>
                        <th>REB</th>
                      </tr>
                    </thead>
                    <tbody>
                      {latestGameDates.map((row) => (
                        <tr key={row.id}>
                          <td>{row.id}</td>
                          <td>{String(row.game_date).slice(0, 10)}</td>
                          <td>{row.player_id}</td>
                          <td>{row.game_id}</td>
                          <td>{row.matchup ?? "-"}</td>
                          <td>{row.points ?? "-"}</td>
                          <td>{row.assists ?? "-"}</td>
                          <td>{row.rebounds ?? "-"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          ) : (
            <div className="admin-recent mt-3">
              <h6 className="mb-2">Latest 5 rows in `mlb_player_game_batting`</h6>
              {latestMlbPlayerGames.length === 0 ? (
                <p className="mb-0 text-muted">No recent MLB batting rows available.</p>
              ) : (
                <div className="table-responsive">
                  <table className="table table-sm mb-0">
                    <thead>
                      <tr>
                        <th>ID</th>
                        <th>Game Date</th>
                        <th>Player</th>
                        <th>Team</th>
                        <th>Game</th>
                        <th>PA</th>
                        <th>H</th>
                        <th>HR</th>
                        <th>TB</th>
                        <th>BB</th>
                        <th>K</th>
                      </tr>
                    </thead>
                    <tbody>
                      {latestMlbPlayerGames.map((row) => (
                        <tr key={row.id}>
                          <td>{row.id}</td>
                          <td>{String(row.game_date).slice(0, 10)}</td>
                          <td>{row.player_name ?? row.player_id}</td>
                          <td>{row.team_abbreviation ?? "-"}</td>
                          <td>{row.game_pk}</td>
                          <td>{row.plate_appearances ?? "-"}</td>
                          <td>{row.hits ?? "-"}</td>
                          <td>{row.home_runs ?? "-"}</td>
                          <td>{row.total_bases ?? "-"}</td>
                          <td>{row.walks ?? "-"}</td>
                          <td>{row.strikeouts ?? "-"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
