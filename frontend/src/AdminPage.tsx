import { useEffect, useMemo, useState } from "react";
import {
  evaluateAllPredictions,
  getLatestIngestionRun,
  getRecentPlayerGameDates,
  ingestGamesByDate,
  recalcUnderRiskAll,
  refreshPlayerTeamAbbr,
  runWalkforwardBacktest,
  startLastNUpdateJob,
  trainAllModels,
  updateRollingFeatures,
  updateTeamGames,
  getUpdateJobStatus,
} from "./api";
import "./AdminPage.css";

type BacktestStat = "points" | "assists" | "rebounds" | "threept" | "threepa";

const BACKTEST_STATS: BacktestStat[] = [
  "points",
  "assists",
  "rebounds",
  "threept",
  "threepa",
];

const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export default function AdminPage() {
  const todayIso = new Date().toISOString().slice(0, 10);
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

  const canRun = useMemo(() => {
    if (isRunning) return false;
    if (!runBacktests) return true;
    return selectedBacktests.length > 0;
  }, [isRunning, runBacktests, selectedBacktests.length]);

  const appendLog = (line: string) => {
    const stamp = new Date().toLocaleTimeString();
    setLogs((prev) => [...prev, `[${stamp}] ${line}`]);
  };

  const toggleBacktest = (stat: BacktestStat) => {
    setSelectedBacktests((prev) =>
      prev.includes(stat) ? prev.filter((s) => s !== stat) : [...prev, stat]
    );
  };

  const runPipeline = async () => {
    setIsRunning(true);
    setLogs([]);
    try {
      if (sinceDate) {
        if (useGameIngest) {
          appendLog(`Starting per-game ingest from ${sinceDate}...`);
          const ingestResult = await ingestGamesByDate({
            since: sinceDate,
            until: untilDate || undefined,
            season,
            include_team_stats: true,
          });
          appendLog(`Game ingest done: ${JSON.stringify(ingestResult)}`);
          if (updateTeamGamesAfterIngest) {
            appendLog("Updating team game logs...");
            const teamResult = await updateTeamGames(season);
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
            appendLog(`Job ${status.status} (${done}/${total})`);
            if (status.status === "completed") break;
            if (status.status === "failed") {
              throw new Error(`Ingest job failed: ${status.error ?? "unknown error"}`);
            }
            await wait(8000);
          }
          appendLog("Per-player ingest completed.");
          if (updateTeamGamesAfterIngest) {
            appendLog("Updating team game logs...");
            const teamResult = await updateTeamGames(season);
            appendLog(`Team games update done: ${JSON.stringify(teamResult)}`);
          }
        }
      } else {
        appendLog("Ingest skipped (no since date provided).");
      }

      if (refreshPlayerTeams) {
        appendLog("Refreshing player team abbreviations...");
        const refreshResult = await refreshPlayerTeamAbbr({
          season,
          fallback: fallbackRefresh,
        });
        appendLog(`Refresh done: ${JSON.stringify(refreshResult)}`);
      }

      if (updateActuals) {
        appendLog("Updating prediction actuals...");
        const actualsResult = await evaluateAllPredictions();
        appendLog(`Actuals update done: ${JSON.stringify(actualsResult)}`);
      }

      if (updateRolling) {
        appendLog("Updating rolling features...");
        const rollingResult = await updateRollingFeatures();
        appendLog(`Rolling update done: ${JSON.stringify(rollingResult)}`);
      }

      if (recalcUnderRisk) {
        appendLog("Recalculating under-risk metrics...");
        const underRiskResult = await recalcUnderRiskAll();
        appendLog(`Under-risk recalc done: ${JSON.stringify(underRiskResult)}`);
      }

      if (trainModels) {
        appendLog("Training models...");
        const trainResult = await trainAllModels();
        appendLog(`Training done: ${JSON.stringify(trainResult)}`);
      } else {
        appendLog("Model training skipped.");
      }

      if (runBacktests) {
        const stats = selectedBacktests.slice();
        appendLog(`Running walk-forward backtests: ${stats.join(", ")}`);
        for (const stat of stats) {
          const backtestResult = await runWalkforwardBacktest(stat, { reset: false });
          appendLog(`${stat} backtest done: ${JSON.stringify(backtestResult)}`);
        }
      } else {
        appendLog("Backtests skipped.");
      }

      appendLog("Pipeline finished successfully.");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      appendLog(`Pipeline failed: ${message}`);
    } finally {
      setIsRunning(false);
    }
  };

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
  }, []);

  return (
    <div className="admin-shell">
      <div className="container py-5">
        <div className="admin-card admin-card-narrow">
          <div className="admin-header">
            <h2 className="mb-1">Data Ingestion Console</h2>
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

          <div className="admin-logs mt-4">
            <div className="admin-log-meta">{latestIngestionText}</div>
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
        </div>
      </div>
    </div>
  );
}
