import { useState } from "react";
import "./App.css";
import {
  getTopScorers,
  getTopAssists,
  getTopRebounders,
  getGuardStats,
  getRecentPerformers,
  getPlayerPropsByGame,
  getPointsPredictions,
  type PlayerRow,
  type PlayerPropsResponse,
  type PointsPrediction,
} from "./api";

// Types derived from the SportsData BettingMarket / BettingOutcome shape.
type BettingOutcome = {
  BettingOutcomeType: string; // Over/Under, Home/Away, etc.
  PayoutAmerican: number;
  Value: number | null;
  Participant: string;
  IsInPlay: boolean;
  SportsbookUrl?: string | null;
  SportsBook?: {
    Name?: string;
  };
};

type BettingMarket = {
  BettingMarketID: number;
  BettingBetType: string; // e.g. "Total Points"
  BettingPeriodType: string; // e.g. "Full Game"
  PlayerName?: string | null;
  TeamKey?: string | null;
   AnyBetsAvailable?: boolean;
   Updated?: string;
  BettingOutcomes?: BettingOutcome[];
};

type NormalizedPropRow = {
  marketId: number;
  player: string;
  team: string | null;
  betType: string;
  period: string;
  outcomeType: string;
  line: number | null;
  odds: number;
  sportsbook: string;
  inPlay: boolean;
};

type TabKey =
  | "top_scorers"
  | "top_assists"
  | "top_rebounders"
  | "guards"
  | "recent"
  | "props"
  | "predictions";

type ApiState<T> = {
  loading: boolean;
  error: string | null;
  data: T | null;
  lastFetched: number | null;
};

const initialState = <T,>(): ApiState<T> => ({
  loading: false,
  error: null,
  data: null,
  lastFetched: null,
});

function formatNumber(value: unknown, digits = 1): string {
  if (typeof value !== "number") return "-";
  return value.toFixed(digits);
}

function normalizeMarkets(markets: BettingMarket[]): NormalizedPropRow[] {
  const rows: NormalizedPropRow[] = [];

  for (const m of markets) {
    const outcomes = m.BettingOutcomes ?? [];
    for (const o of outcomes) {
      rows.push({
        marketId: m.BettingMarketID,
        player: m.PlayerName ?? o.Participant,
        team: m.TeamKey ?? null,
        betType: m.BettingBetType,
        period: m.BettingPeriodType,
        outcomeType: o.BettingOutcomeType,
        line: o.Value ?? null,
        odds: o.PayoutAmerican,
        sportsbook: o.SportsBook?.Name ?? "",
        inPlay: o.IsInPlay,
      });
    }
  }

  return rows;
}

function PlayerTable({ rows }: { rows: PlayerRow[] }) {
  if (!rows.length) return <p className="muted">No data.</p>;

  return (
    <div className="table-wrapper">
      <table>
        <thead>
          <tr>
            <th>Player</th>
            <th>Team</th>
            <th>GP</th>
            <th>MIN</th>
            <th>PTS</th>
            <th>REB</th>
            <th>AST</th>
            <th>Fantasy</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.PLAYER_ID}>
              <td>{row.PLAYER_NAME}</td>
              <td>{row.TEAM_ABBREVIATION ?? "-"}</td>
              <td>{row.GP ?? "-"}</td>
              <td>{formatNumber(row.MIN)}</td>
              <td>{formatNumber(row.PTS)}</td>
              <td>{formatNumber(row.REB)}</td>
              <td>{formatNumber(row.AST)}</td>
              <td>{formatNumber(row.NBA_FANTASY_PTS)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function PredictionsGrid({ predictions }: { predictions: PointsPrediction[] }) {
  if (!predictions.length) return <p className="muted">No predictions available.</p>;

  const formatDate = (dateString: string) => {
    const date = new Date(dateString);
    return date.toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
    });
  };

  return (
    <div className="predictions-grid">
      {predictions.map((pred) => (
        <div key={pred.player_id} className="prediction-card">
          <div className="prediction-card-header">
            <div className="prediction-player-info">
              <h3 className="prediction-player-name">{pred.full_name}</h3>
              <span className="prediction-team-badge">{pred.team_abbreviation}</span>
            </div>
            <div className="prediction-points">
              <span className="prediction-points-value">
                {pred.pred_points.toFixed(1)}
              </span>
              <span className="prediction-points-label">pts</span>
            </div>
          </div>
          <div className="prediction-card-body">
            <div className="prediction-matchup">
              <span className="prediction-matchup-icon">🏀</span>
              <span>{pred.matchup}</span>
            </div>
            <div className="prediction-date">
              <span className="prediction-date-icon">📅</span>
              <span>{formatDate(pred.game_date)}</span>
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}

function App() {
  const [activeTab, setActiveTab] = useState<TabKey>("top_scorers");

  const [topScorers, setTopScorers] =
    useState<ApiState<PlayerRow[]>>(initialState);
  const [topAssists, setTopAssists] =
    useState<ApiState<PlayerRow[]>>(initialState);
  const [topRebounders, setTopRebounders] =
    useState<ApiState<PlayerRow[]>>(initialState);
  const [guards, setGuards] = useState<ApiState<PlayerRow[]>>(initialState);
  const [recent, setRecent] = useState<ApiState<PlayerRow[]>>(initialState);
  const [propsState, setPropsState] =
    useState<ApiState<PlayerPropsResponse>>(initialState);
  const [gameIdInput, setGameIdInput] = useState("");
  const [predictionsState, setPredictionsState] =
    useState<ApiState<PointsPrediction[]>>(initialState);
  const [predictionDay, setPredictionDay] = useState<
    "today" | "tomorrow" | "yesterday"
  >("today");

  // Helper to avoid hammering the backend. Enforces a minimum interval between
  // network calls per section while keeping the UI logic simple.
  async function safeLoad<T>(
    state: ApiState<T>,
    setState: (s: ApiState<T>) => void,
    loader: () => Promise<T>,
    minIntervalMs: number
  ) {
    const now = Date.now();
    if (state.lastFetched && now - state.lastFetched < minIntervalMs) {
      // Within cooldown window, just reuse data and skip a new API call
      return;
    }
    try {
      setState({ ...state, loading: true, error: null });
      const data = await loader();
      setState({
        loading: false,
        error: null,
        data,
        lastFetched: now,
      });
    } catch (err) {
      const message =
        err instanceof Error ? err.message : "Unexpected error occurred";
      setState({
        ...state,
        loading: false,
        error: message,
      });
    }
  }

  const handleLoadTopScorers = () =>
    safeLoad(topScorers, setTopScorers, () => getTopScorers(), 5 * 60 * 1000);
  const handleLoadTopAssists = () =>
    safeLoad(topAssists, setTopAssists, () => getTopAssists(), 5 * 60 * 1000);
  const handleLoadTopRebounders = () =>
    safeLoad(
      topRebounders,
      setTopRebounders,
      () => getTopRebounders(),
      5 * 60 * 1000
    );
  const handleLoadGuards = () =>
    safeLoad(guards, setGuards, () => getGuardStats(), 5 * 60 * 1000);
  const handleLoadRecent = () =>
    safeLoad(
      recent,
      setRecent,
      () => getRecentPerformers(),
      5 * 60 * 1000
    );

  const handleLoadProps = () => {
    const gameId = Number(gameIdInput);
    if (!gameId || Number.isNaN(gameId)) {
      setPropsState({
        ...propsState,
        error: "Enter a valid numeric game ID.",
      });
      return;
    }
    return safeLoad(
      propsState,
      setPropsState,
      () => getPlayerPropsByGame(gameId),
      60 * 60 * 1000 // 1 hour cooldown for props
    );
  };

  const handleLoadPredictions = () =>
    safeLoad(
      predictionsState,
      setPredictionsState,
      () => getPointsPredictions(predictionDay),
      5 * 60 * 1000 // 5 minute cooldown for predictions
    );

  const renderContent = () => {
    switch (activeTab) {
      case "top_scorers":
        return (
          <section className="card">
            <header className="card-header">
              <div>
                <h2>Top Scorers</h2>
                <p className="muted">Per-game points leaders.</p>
              </div>
              <button onClick={handleLoadTopScorers} disabled={topScorers.loading}>
                {topScorers.loading ? "Loading..." : "Refresh data"}
              </button>
            </header>
            {topScorers.error && (
              <p className="error">Error: {topScorers.error}</p>
            )}
            {topScorers.data && <PlayerTable rows={topScorers.data} />}
          </section>
        );
      case "top_assists":
        return (
          <section className="card">
            <header className="card-header">
              <div>
                <h2>Top Playmakers</h2>
                <p className="muted">Assist leaders.</p>
              </div>
              <button onClick={handleLoadTopAssists} disabled={topAssists.loading}>
                {topAssists.loading ? "Loading..." : "Refresh data"}
              </button>
            </header>
            {topAssists.error && (
              <p className="error">Error: {topAssists.error}</p>
            )}
            {topAssists.data && <PlayerTable rows={topAssists.data} />}
          </section>
        );
      case "top_rebounders":
        return (
          <section className="card">
            <header className="card-header">
              <div>
                <h2>Top Rebounders</h2>
                <p className="muted">Rebound leaders.</p>
              </div>
              <button
                onClick={handleLoadTopRebounders}
                disabled={topRebounders.loading}
              >
                {topRebounders.loading ? "Loading..." : "Refresh data"}
              </button>
            </header>
            {topRebounders.error && (
              <p className="error">Error: {topRebounders.error}</p>
            )}
            {topRebounders.data && <PlayerTable rows={topRebounders.data} />}
          </section>
        );
      case "guards":
        return (
          <section className="card">
            <header className="card-header">
              <div>
                <h2>Guards Snapshot</h2>
                <p className="muted">Guard scoring and fantasy output.</p>
              </div>
              <button onClick={handleLoadGuards} disabled={guards.loading}>
                {guards.loading ? "Loading..." : "Refresh data"}
              </button>
            </header>
            {guards.error && <p className="error">Error: {guards.error}</p>}
            {guards.data && <PlayerTable rows={guards.data} />}
          </section>
        );
      case "recent":
        return (
          <section className="card">
            <header className="card-header">
              <div>
                <h2>Recent Performers</h2>
                <p className="muted">Recent form based on fantasy production.</p>
              </div>
              <button onClick={handleLoadRecent} disabled={recent.loading}>
                {recent.loading ? "Loading..." : "Refresh data"}
              </button>
            </header>
            {recent.error && <p className="error">Error: {recent.error}</p>}
            {recent.data && <PlayerTable rows={recent.data} />}
          </section>
        );
      case "props":
        return (
          <section className="card">
            <header className="card-header">
              <div>
                <h2>Player Props (by Game)</h2>
                <p className="muted">
                  Snapshot of available player prop markets for a game.
                </p>
              </div>
            </header>
            <div className="props-controls">
              <input
                type="number"
                placeholder="Game ID"
                value={gameIdInput}
                onChange={(e) => setGameIdInput(e.target.value)}
              />
              <button onClick={handleLoadProps} disabled={propsState.loading}>
                {propsState.loading ? "Loading..." : "Refresh data"}
              </button>
            </div>
            {propsState.error && (
              <p className="error">Error: {propsState.error}</p>
            )}
            {propsState.data && (
              <>
                <div className="props-summary">
                  <p>
                    <strong>Game ID:</strong> {propsState.data.game_id}
                  </p>
                  <p>
                    <strong>Markets:</strong> {propsState.data.count}
                  </p>
                </div>
                <div className="table-wrapper" style={{ marginTop: "0.75rem" }}>
                  {(() => {
                    const markets = propsState.data
                      .markets as BettingMarket[];
                    const rows = normalizeMarkets(markets);

                    // If we have fully populated outcomes, show the detailed table.
                    if (rows.length) {
                      return (
                        <table>
                          <thead>
                            <tr>
                              <th>Player</th>
                              <th>Team</th>
                              <th>Bet</th>
                              <th>Period</th>
                              <th>Side</th>
                              <th>Line</th>
                              <th>Odds</th>
                              <th>Book</th>
                              <th>In-Play</th>
                            </tr>
                          </thead>
                          <tbody>
                            {rows.map((row, index) => (
                              <tr key={`${row.marketId}-${index}`}>
                                <td>{row.player}</td>
                                <td>{row.team ?? "-"}</td>
                                <td>{row.betType}</td>
                                <td>{row.period}</td>
                                <td>{row.outcomeType}</td>
                                <td>
                                  {row.line !== null ? row.line.toFixed(1) : "-"}
                                </td>
                                <td>
                                  {row.odds > 0 ? `+${row.odds}` : row.odds}
                                </td>
                                <td>{row.sportsbook}</td>
                                <td>{row.inPlay ? "Yes" : "No"}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      );
                    }

                    // Fallback: show market-level view when there are no outcomes yet.
                    if (!markets.length) {
                      return <p className="muted">No markets available.</p>;
                    }

                    return (
                      <table>
                        <thead>
                          <tr>
                            <th>Player</th>
                            <th>Team</th>
                            <th>Bet</th>
                            <th>Period</th>
                            <th>Any Bets</th>
                            <th>Updated</th>
                          </tr>
                        </thead>
                        <tbody>
                          {markets.map((m) => (
                            <tr key={m.BettingMarketID}>
                              <td>{m.PlayerName ?? "-"}</td>
                              <td>{m.TeamKey ?? "-"}</td>
                              <td>{m.BettingBetType}</td>
                              <td>{m.BettingPeriodType}</td>
                              <td>{m.AnyBetsAvailable ? "Yes" : "No"}</td>
                              <td>{m.Updated}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    );
                  })()}
                </div>
              </>
            )}
          </section>
        );
      case "predictions":
        return (
          <section className="card">
            <header className="card-header">
              <div>
                <h2>Points Predictions</h2>
                <p className="muted">ML-powered predictions for player points.</p>
              </div>
              <div style={{ display: "flex", gap: "0.75rem", alignItems: "center" }}>
                <select
                  value={predictionDay}
                  onChange={(e) =>
                    setPredictionDay(
                      e.target.value as "today" | "tomorrow" | "yesterday"
                    )
                  }
                  style={{
                    borderRadius: "999px",
                    border: "1px solid #d1d5db",
                    padding: "0.45rem 0.75rem",
                    fontSize: "0.9rem",
                    background: "#ffffff",
                  }}
                >
                  <option value="today">Today</option>
                  <option value="tomorrow">Tomorrow</option>
                  <option value="yesterday">Yesterday</option>
                </select>
                <button
                  onClick={handleLoadPredictions}
                  disabled={predictionsState.loading}
                >
                  {predictionsState.loading ? "Loading..." : "Get Predictions"}
                </button>
              </div>
            </header>
            {predictionsState.error && (
              <p className="error">Error: {predictionsState.error}</p>
            )}
            {predictionsState.data && (
              <PredictionsGrid predictions={predictionsState.data} />
            )}
          </section>
        );
      default:
        return null;
    }
  };

  return (
    <div className="app-root">
      <header className="top-bar">
        <div>
          <h1>NBA Betting Dashboard</h1>
          <p className="muted">
            Clean, read-only view of NBA player performance and betting markets.
          </p>
        </div>
      </header>

      <nav className="tabs">
        <button
          className={activeTab === "top_scorers" ? "tab active" : "tab"}
          onClick={() => setActiveTab("top_scorers")}
        >
          Top Scorers
        </button>
        <button
          className={activeTab === "top_assists" ? "tab active" : "tab"}
          onClick={() => setActiveTab("top_assists")}
        >
          Assists
        </button>
        <button
          className={activeTab === "top_rebounders" ? "tab active" : "tab"}
          onClick={() => setActiveTab("top_rebounders")}
        >
          Rebounds
        </button>
        <button
          className={activeTab === "guards" ? "tab active" : "tab"}
          onClick={() => setActiveTab("guards")}
        >
          Guards
        </button>
        <button
          className={activeTab === "recent" ? "tab active" : "tab"}
          onClick={() => setActiveTab("recent")}
        >
          Recent
        </button>
        <button
          className={activeTab === "props" ? "tab active" : "tab"}
          onClick={() => setActiveTab("props")}
        >
          Player Props
        </button>
        <button
          className={activeTab === "predictions" ? "tab active" : "tab"}
          onClick={() => setActiveTab("predictions")}
        >
          Predictions
        </button>
      </nav>

      <main>{renderContent()}</main>

      <footer className="footer">
        <p className="muted">
          Backend: FastAPI · Frontend: React + Vite.
        </p>
      </footer>
    </div>
  );
}

export default App;

