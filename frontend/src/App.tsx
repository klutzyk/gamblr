import { useEffect, useState } from "react";
import "./App.css";
import logo from "./assets/logo.jpg";
import {
  getTopScorers,
  getTopAssists,
  getTopRebounders,
  getGuardStats,
  getRecentPerformers,
  getPlayerPropsByGame,
  getPointsPredictions,
  getAssistsPredictions,
  getReboundsPredictions,
  type PlayerRow,
  type PlayerPropsResponse,
  type PredictionRow,
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
  if (!rows.length) {
    return (
      <div className="text-center py-5">
        <p className="text-secondary">No data available.</p>
      </div>
    );
  }

  return (
    <div className="table-responsive">
      <table className="table align-items-center mb-0">
        <thead>
          <tr>
            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Player</th>
            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Team</th>
            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">GP</th>
            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">MIN</th>
            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">PTS</th>
            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">REB</th>
            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">AST</th>
            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Fantasy</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.PLAYER_ID}>
              <td>
                <div className="d-flex px-2 py-1">
                  <div className="d-flex flex-column justify-content-center">
                    <h6 className="mb-0 text-sm">{row.PLAYER_NAME}</h6>
                  </div>
                </div>
              </td>
              <td>
                <span className="badge badge-sm bg-gradient-primary">
                  {row.TEAM_ABBREVIATION ?? "-"}
                </span>
              </td>
              <td className="align-middle text-center text-sm">
                <span className="text-secondary font-weight-bold">{row.GP ?? "-"}</span>
              </td>
              <td className="align-middle text-center text-sm">
                <span className="text-secondary font-weight-bold">{formatNumber(row.MIN)}</span>
              </td>
              <td className="align-middle text-center text-sm">
                <span className="text-primary font-weight-bold">{formatNumber(row.PTS)}</span>
              </td>
              <td className="align-middle text-center text-sm">
                <span className="text-secondary font-weight-bold">{formatNumber(row.REB)}</span>
              </td>
              <td className="align-middle text-center text-sm">
                <span className="text-secondary font-weight-bold">{formatNumber(row.AST)}</span>
              </td>
              <td className="align-middle text-center text-sm">
                <span className="text-success font-weight-bold">{formatNumber(row.NBA_FANTASY_PTS)}</span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
function PredictionsGrid({
  predictions,
  statLabel,
  unitLabel,
}: {
  predictions: PredictionRow[];
  statLabel: string;
  unitLabel: string;
}) {
  const TEAM_ID_BY_ABBR: Record<string, number> = {
    ATL: 1610612737,
    BOS: 1610612738,
    BKN: 1610612751,
    CHA: 1610612766,
    CHI: 1610612741,
    CLE: 1610612739,
    DAL: 1610612742,
    DEN: 1610612743,
    DET: 1610612765,
    GSW: 1610612744,
    HOU: 1610612745,
    IND: 1610612754,
    LAC: 1610612746,
    LAL: 1610612747,
    MEM: 1610612763,
    MIA: 1610612748,
    MIL: 1610612749,
    MIN: 1610612750,
    NOP: 1610612740,
    NYK: 1610612752,
    OKC: 1610612760,
    ORL: 1610612753,
    PHI: 1610612755,
    PHX: 1610612756,
    POR: 1610612757,
    SAC: 1610612758,
    SAS: 1610612759,
    TOR: 1610612761,
    UTA: 1610612762,
    WAS: 1610612764,
  };
  if (!predictions.length) {
    return (
      <div className="text-center py-5">
        <p className="text-secondary">No predictions available.</p>
      </div>
    );
  }

  const formatDate = (dateString: string) => {
    const date = new Date(dateString);
    return date.toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
    });
  };

  const getInitials = (name: string) =>
    name
      .split(" ")
      .filter(Boolean)
      .slice(0, 2)
      .map((part) => part[0])
      .join("")
      .toUpperCase();

  const getHeadshotUrl = (
    playerId: number,
    teamId?: number,
    teamAbbr?: string
  ) => {
    const resolvedTeamId =
      teamId ?? (teamAbbr ? TEAM_ID_BY_ABBR[teamAbbr] : undefined);
    if (!resolvedTeamId) return "";
    return `https://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/${resolvedTeamId}/2025/260x190/${playerId}.png`;
  };

  return (
    <div className="predictions-grid mt-4">
      {predictions.map((pred) => {
        const confidenceValue =
          typeof pred.confidence === "number" ? pred.confidence : null;
        let confidenceClass = "confidence-neutral";
        if (confidenceValue !== null) {
          if (confidenceValue >= 75) confidenceClass = "confidence-high";
          else if (confidenceValue >= 55) confidenceClass = "confidence-mid";
          else confidenceClass = "confidence-low";
        }
        const headshotUrl = getHeadshotUrl(
          pred.player_id,
          pred.team_id,
          pred.team_abbreviation
        );
        return (
        <div key={pred.player_id} className="card card-body border-radius-xl shadow-lg">
          <div className="d-flex justify-content-between align-items-start mb-3">
            <div className="flex-grow-1">
              <div className="d-flex align-items-center gap-2 mb-1">
                <div className="player-avatar">
                  {headshotUrl && (
                    <img
                      src={headshotUrl}
                      alt={pred.full_name}
                      onError={(e) => {
                        e.currentTarget.style.display = "none";
                      }}
                    />
                  )}
                  <span className="avatar-fallback">
                    {getInitials(pred.full_name)}
                  </span>
                </div>
                <div>
                  <h5 className="mb-1">{pred.full_name}</h5>
                  <span className="badge badge-sm bg-gradient-primary mb-2">
                    {pred.team_abbreviation}
                  </span>
                </div>
              </div>
            </div>
            <div className="text-end">
              <h2 className="mb-0 text-gradient text-primary">
                {pred.pred_value.toFixed(1)}
              </h2>
              <span className="text-xs text-secondary">{unitLabel}</span>
            </div>
          </div>
          <div className="border-top pt-3">
            <div className="d-flex align-items-center mb-2">
              <i className="material-symbols-rounded text-primary me-2">sports_basketball</i>
              <span className="text-sm font-weight-bold">{pred.matchup}</span>
            </div>
            <div className="d-flex align-items-center">
              <i className="material-symbols-rounded text-secondary me-2">calendar_today</i>
              <span className="text-sm text-secondary">{formatDate(pred.game_date)}</span>
            </div>
            <div className="prediction-stat-tag mt-3">
              <span className="badge badge-sm bg-gradient-info">{statLabel}</span>
            </div>
            {typeof pred.pred_p10 === "number" &&
              typeof pred.pred_p90 === "number" && (
                <div className={`prediction-band mt-3 ${confidenceClass}`}>
                  <div className="prediction-band-row">
                    <span className="label">Confidence band</span>
                    <span className="value">
                      {pred.pred_p10.toFixed(1)} – {pred.pred_p90.toFixed(1)} {unitLabel}
                    </span>
                  </div>
                  {typeof pred.confidence === "number" && (
                    <div className="prediction-confidence">
                      <span>Confidence</span>
                      <strong>{pred.confidence}%</strong>
                    </div>
                  )}
                  <div className="prediction-range">
                    <div
                      className={`prediction-range-fill ${confidenceClass}`}
                      style={{
                        left: "10%",
                        right: "10%",
                      }}
                    ></div>
                    <span className="prediction-marker">
                      {pred.pred_p50?.toFixed(1) ?? pred.pred_value.toFixed(1)}
                    </span>
                  </div>
                </div>
              )}
          </div>
        </div>
        );
      })}
    </div>
  );
}

function App() {
  const [activeTab, setActiveTab] = useState<TabKey>("predictions");

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
  const [predictionStat, setPredictionStat] = useState<
    "points" | "assists" | "rebounds"
  >("points");
  const [pointsPredictionsState, setPointsPredictionsState] =
    useState<ApiState<PredictionRow[]>>(initialState);
  const [assistsPredictionsState, setAssistsPredictionsState] =
    useState<ApiState<PredictionRow[]>>(initialState);
  const [reboundsPredictionsState, setReboundsPredictionsState] =
    useState<ApiState<PredictionRow[]>>(initialState);
  const [predictionDay, setPredictionDay] = useState<
    "today" | "tomorrow" | "yesterday" | "auto"
  >("auto");
  const [predictionSort, setPredictionSort] = useState<
    "pred_value_desc" | "pred_value_asc" | "confidence_desc"
  >("pred_value_desc");
  const [predictionSearch, setPredictionSearch] = useState("");
  const [predictionTeam, setPredictionTeam] = useState("all");

  // Helper to avoid hammering the backend. Enforces a minimum interval between
  // network calls per section while keeping the UI logic simple.
  async function safeLoad<T>(
    state: ApiState<T>,
    setState: (s: ApiState<T>) => void,
    loader: () => Promise<T>,
    minIntervalMs: number,
    force = false
  ) {
    const now = Date.now();
    if (!force && state.lastFetched && now - state.lastFetched < minIntervalMs) {
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

  const predictionConfig = {
    points: {
      label: "Points",
      unit: "pts",
      state: pointsPredictionsState,
      setState: setPointsPredictionsState,
      loader: () => getPointsPredictions(predictionDay),
    },
    assists: {
      label: "Assists",
      unit: "ast",
      state: assistsPredictionsState,
      setState: setAssistsPredictionsState,
      loader: () => getAssistsPredictions(predictionDay),
    },
    rebounds: {
      label: "Rebounds",
      unit: "reb",
      state: reboundsPredictionsState,
      setState: setReboundsPredictionsState,
      loader: () => getReboundsPredictions(predictionDay),
    },
  };

  const handleLoadPredictions = (force = false) => {
    const config = predictionConfig[predictionStat];
    return safeLoad(
      config.state,
      config.setState,
      config.loader,
      5 * 60 * 1000,
      force
    );
  };

  useEffect(() => {
    handleLoadPredictions(true);
  }, [predictionDay, predictionStat]);

  const renderContent = () => {
    switch (activeTab) {
      case "top_scorers":
        return (
          <div className="card card-body border-radius-xl shadow-lg">
            <div className="d-flex justify-content-between align-items-center mb-4">
              <div>
                <h4 className="mb-1">Top Scorers</h4>
                <p className="text-sm text-secondary mb-0">Per-game points leaders.</p>
              </div>
              <button
                className="btn btn-sm bg-gradient-primary mb-0"
                onClick={handleLoadTopScorers}
                disabled={topScorers.loading}
              >
                {topScorers.loading ? (
                  <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                ) : (
                  <i className="material-symbols-rounded me-2" style={{ fontSize: "16px" }}>refresh</i>
                )}
                Refresh
              </button>
            </div>
            {topScorers.error && (
              <div className="alert alert-danger text-white" role="alert">
                <strong>Error:</strong> {topScorers.error}
              </div>
            )}
            {topScorers.loading && !topScorers.data && (
              <div className="text-center py-5">
                <div className="spinner-border text-primary" role="status">
                  <span className="visually-hidden">Loading...</span>
                </div>
                <p className="text-secondary mt-3">Loading data...</p>
              </div>
            )}
            {topScorers.data && <PlayerTable rows={topScorers.data} />}
          </div>
        );
      case "top_assists":
        return (
          <div className="card card-body border-radius-xl shadow-lg">
            <div className="d-flex justify-content-between align-items-center mb-4">
              <div>
                <h4 className="mb-1">Top Playmakers</h4>
                <p className="text-sm text-secondary mb-0">Assist leaders.</p>
              </div>
              <button
                className="btn btn-sm bg-gradient-primary mb-0"
                onClick={handleLoadTopAssists}
                disabled={topAssists.loading}
              >
                {topAssists.loading ? (
                  <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                ) : (
                  <i className="material-symbols-rounded me-2" style={{ fontSize: "16px" }}>refresh</i>
                )}
                Refresh
              </button>
            </div>
            {topAssists.error && (
              <div className="alert alert-danger text-white" role="alert">
                <strong>Error:</strong> {topAssists.error}
              </div>
            )}
            {topAssists.loading && !topAssists.data && (
              <div className="text-center py-5">
                <div className="spinner-border text-primary" role="status">
                  <span className="visually-hidden">Loading...</span>
                </div>
                <p className="text-secondary mt-3">Loading data...</p>
              </div>
            )}
            {topAssists.data && <PlayerTable rows={topAssists.data} />}
          </div>
        );
      case "top_rebounders":
        return (
          <div className="card card-body border-radius-xl shadow-lg">
            <div className="d-flex justify-content-between align-items-center mb-4">
              <div>
                <h4 className="mb-1">Top Rebounders</h4>
                <p className="text-sm text-secondary mb-0">Rebound leaders.</p>
              </div>
              <button
                className="btn btn-sm bg-gradient-primary mb-0"
                onClick={handleLoadTopRebounders}
                disabled={topRebounders.loading}
              >
                {topRebounders.loading ? (
                  <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                ) : (
                  <i className="material-symbols-rounded me-2" style={{ fontSize: "16px" }}>refresh</i>
                )}
                Refresh
              </button>
            </div>
            {topRebounders.error && (
              <div className="alert alert-danger text-white" role="alert">
                <strong>Error:</strong> {topRebounders.error}
              </div>
            )}
            {topRebounders.loading && !topRebounders.data && (
              <div className="text-center py-5">
                <div className="spinner-border text-primary" role="status">
                  <span className="visually-hidden">Loading...</span>
                </div>
                <p className="text-secondary mt-3">Loading data...</p>
              </div>
            )}
            {topRebounders.data && <PlayerTable rows={topRebounders.data} />}
          </div>
        );
      case "guards":
        return (
          <div className="card card-body border-radius-xl shadow-lg">
            <div className="d-flex justify-content-between align-items-center mb-4">
              <div>
                <h4 className="mb-1">Guards Snapshot</h4>
                <p className="text-sm text-secondary mb-0">Guard scoring and fantasy output.</p>
              </div>
              <button
                className="btn btn-sm bg-gradient-primary mb-0"
                onClick={handleLoadGuards}
                disabled={guards.loading}
              >
                {guards.loading ? (
                  <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                ) : (
                  <i className="material-symbols-rounded me-2" style={{ fontSize: "16px" }}>refresh</i>
                )}
                Refresh
              </button>
            </div>
            {guards.error && (
              <div className="alert alert-danger text-white" role="alert">
                <strong>Error:</strong> {guards.error}
              </div>
            )}
            {guards.loading && !guards.data && (
              <div className="text-center py-5">
                <div className="spinner-border text-primary" role="status">
                  <span className="visually-hidden">Loading...</span>
                </div>
                <p className="text-secondary mt-3">Loading data...</p>
              </div>
            )}
            {guards.data && <PlayerTable rows={guards.data} />}
          </div>
        );
      case "recent":
        return (
          <div className="card card-body border-radius-xl shadow-lg">
            <div className="d-flex justify-content-between align-items-center mb-4">
              <div>
                <h4 className="mb-1">Recent Performers</h4>
                <p className="text-sm text-secondary mb-0">Recent form based on fantasy production.</p>
              </div>
              <button
                className="btn btn-sm bg-gradient-primary mb-0"
                onClick={handleLoadRecent}
                disabled={recent.loading}
              >
                {recent.loading ? (
                  <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                ) : (
                  <i className="material-symbols-rounded me-2" style={{ fontSize: "16px" }}>refresh</i>
                )}
                Refresh
              </button>
            </div>
            {recent.error && (
              <div className="alert alert-danger text-white" role="alert">
                <strong>Error:</strong> {recent.error}
              </div>
            )}
            {recent.loading && !recent.data && (
              <div className="text-center py-5">
                <div className="spinner-border text-primary" role="status">
                  <span className="visually-hidden">Loading...</span>
                </div>
                <p className="text-secondary mt-3">Loading data...</p>
              </div>
            )}
            {recent.data && <PlayerTable rows={recent.data} />}
          </div>
        );
      case "props":
        return (
          <div className="card card-body border-radius-xl shadow-lg">
            <div className="mb-4">
              <h4 className="mb-1">Player Props (by Game)</h4>
              <p className="text-sm text-secondary mb-0">
                Snapshot of available player prop markets for a game.
              </p>
            </div>
            <div className="d-flex flex-wrap gap-2 mb-4">
              <input
                type="number"
                className="form-control"
                placeholder="Game ID"
                value={gameIdInput}
                onChange={(e) => setGameIdInput(e.target.value)}
                style={{ maxWidth: "200px" }}
              />
              <button
                className="btn btn-sm bg-gradient-primary mb-0"
                onClick={handleLoadProps}
                disabled={propsState.loading}
              >
                {propsState.loading ? (
                  <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                ) : (
                  <i className="material-symbols-rounded me-2" style={{ fontSize: "16px" }}>search</i>
                )}
                Load Props
              </button>
            </div>
            {propsState.error && (
              <div className="alert alert-danger text-white" role="alert">
                <strong>Error:</strong> {propsState.error}
              </div>
            )}
            {propsState.loading && !propsState.data && (
              <div className="text-center py-5">
                <div className="spinner-border text-primary" role="status">
                  <span className="visually-hidden">Loading...</span>
                </div>
                <p className="text-secondary mt-3">Loading data...</p>
              </div>
            )}
            {propsState.data && (
              <>
                <div className="d-flex gap-4 mb-4">
                  <div>
                    <span className="text-sm text-secondary">Game ID:</span>
                    <span className="text-sm font-weight-bold ms-2">{propsState.data.game_id}</span>
                  </div>
                  <div>
                    <span className="text-sm text-secondary">Markets:</span>
                    <span className="text-sm font-weight-bold ms-2">{propsState.data.count}</span>
                  </div>
                </div>
                <div className="table-responsive">
                  {(() => {
                    const markets = propsState.data
                      .markets as BettingMarket[];
                    const rows = normalizeMarkets(markets);

                    // If we have fully populated outcomes, show the detailed table.
                    if (rows.length) {
                      return (
                        <table className="table align-items-center mb-0">
                          <thead>
                            <tr>
                              <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Player</th>
                              <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Team</th>
                              <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Bet</th>
                              <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Period</th>
                              <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Side</th>
                              <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Line</th>
                              <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Odds</th>
                              <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Book</th>
                              <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">In-Play</th>
                            </tr>
                          </thead>
                          <tbody>
                            {rows.map((row, index) => (
                              <tr key={`${row.marketId}-${index}`}>
                                <td className="text-sm">{row.player}</td>
                                <td>
                                  <span className="badge badge-sm bg-gradient-secondary">
                                    {row.team ?? "-"}
                                  </span>
                                </td>
                                <td className="text-sm">{row.betType}</td>
                                <td className="text-sm">{row.period}</td>
                                <td className="text-sm">{row.outcomeType}</td>
                                <td className="text-sm">
                                  {row.line !== null ? row.line.toFixed(1) : "-"}
                                </td>
                                <td className="text-sm">
                                  {row.odds > 0 ? `+${row.odds}` : row.odds}
                                </td>
                                <td className="text-sm">{row.sportsbook}</td>
                                <td>
                                  {row.inPlay ? (
                                    <span className="badge badge-sm bg-gradient-success">Yes</span>
                                  ) : (
                                    <span className="badge badge-sm bg-gradient-secondary">No</span>
                                  )}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      );
                    }

                    // Fallback: show market-level view when there are no outcomes yet.
                    if (!markets.length) {
                      return <p className="text-secondary text-center py-4">No markets available.</p>;
                    }

                    return (
                      <table className="table align-items-center mb-0">
                        <thead>
                          <tr>
                            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Player</th>
                            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Team</th>
                            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Bet</th>
                            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Period</th>
                            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Any Bets</th>
                            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Updated</th>
                          </tr>
                        </thead>
                        <tbody>
                          {markets.map((m) => (
                            <tr key={m.BettingMarketID}>
                              <td className="text-sm">{m.PlayerName ?? "-"}</td>
                              <td className="text-sm">{m.TeamKey ?? "-"}</td>
                              <td className="text-sm">{m.BettingBetType}</td>
                              <td className="text-sm">{m.BettingPeriodType}</td>
                              <td>
                                {m.AnyBetsAvailable ? (
                                  <span className="badge badge-sm bg-gradient-success">Yes</span>
                                ) : (
                                  <span className="badge badge-sm bg-gradient-secondary">No</span>
                                )}
                              </td>
                              <td className="text-sm">{m.Updated}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    );
                  })()}
                </div>
              </>
            )}
          </div>
        );
      case "predictions":
        const activePrediction = predictionConfig[predictionStat];
        const teamOptions = activePrediction.state.data
          ? Array.from(
              new Set(
                activePrediction.state.data
                  .map((row) => row.team_abbreviation)
                  .filter((team): team is string => Boolean(team))
              )
            ).sort()
          : [];
        const normalizedSearch = predictionSearch.trim().toLowerCase();
        const filteredPredictions = activePrediction.state.data
          ? activePrediction.state.data.filter((row) => {
              if (
                predictionTeam !== "all" &&
                row.team_abbreviation !== predictionTeam
              ) {
                return false;
              }
              if (!normalizedSearch) {
                return true;
              }
              const nameMatch = row.full_name
                ?.toLowerCase()
                .includes(normalizedSearch);
              const teamMatch = row.team_abbreviation
                ?.toLowerCase()
                .includes(normalizedSearch);
              return Boolean(nameMatch || teamMatch);
            })
          : [];
        return (
          <div className="card card-body border-radius-xl shadow-lg prediction-focus">
            <div className="d-flex flex-column flex-lg-row justify-content-between align-items-start gap-3 mb-4">
              <div>
                <h4 className="mb-1">Prediction Focus</h4>
                <p className="text-sm text-secondary mb-0">
                  Live projections for points, assists, and rebounds.
                </p>
              </div>
              <div className="d-flex flex-wrap gap-2 align-items-center">
                <div className="stat-toggle">
                  {(["points", "assists", "rebounds"] as const).map((stat) => (
                    <button
                      key={stat}
                      className={`stat-chip ${
                        predictionStat === stat ? "active" : ""
                      }`}
                      onClick={() => setPredictionStat(stat)}
                    >
                      {predictionConfig[stat].label}
                    </button>
                  ))}
                </div>
                <select
                  className="form-select form-select-sm"
                  value={predictionDay}
                  onChange={(e) =>
                    setPredictionDay(
                      e.target.value as "today" | "tomorrow" | "yesterday" | "auto"
                    )
                  }
                  style={{ maxWidth: "150px" }}
                >
                  <option value="auto">Auto (ET)</option>
                  <option value="today">Today (ET)</option>
                  <option value="tomorrow">Tomorrow (ET)</option>
                  <option value="yesterday">Yesterday (ET)</option>
                </select>
                <select
                  className="form-select form-select-sm"
                  value={predictionSort}
                  onChange={(e) =>
                    setPredictionSort(
                      e.target.value as
                        | "pred_value_desc"
                        | "pred_value_asc"
                        | "confidence_desc"
                    )
                  }
                  style={{ maxWidth: "170px" }}
                >
                  <option value="pred_value_desc">Value (High → Low)</option>
                  <option value="pred_value_asc">Value (Low → High)</option>
                  <option value="confidence_desc">Confidence (High → Low)</option>
                </select>
                <button
                  className="btn btn-sm bg-gradient-primary mb-0"
                  onClick={handleLoadPredictions}
                  disabled={activePrediction.state.loading}
                >
                  {activePrediction.state.loading ? (
                    <span
                      className="spinner-border spinner-border-sm me-2"
                      role="status"
                      aria-hidden="true"
                    ></span>
                  ) : (
                    <i
                      className="material-symbols-rounded me-2"
                      style={{ fontSize: "16px" }}
                    >
                      psychology
                    </i>
                  )}
                  Get Predictions
                </button>
              </div>
            </div>
            <div className="prediction-filters mb-4">
              <div className="prediction-search">
                <i className="material-symbols-rounded">search</i>
                <input
                  type="search"
                  placeholder="Search player or team"
                  value={predictionSearch}
                  onChange={(e) => setPredictionSearch(e.target.value)}
                />
              </div>
              <div className="prediction-team-chips">
                <button
                  className={`team-chip ${
                    predictionTeam === "all" ? "active" : ""
                  }`}
                  onClick={() => setPredictionTeam("all")}
                >
                  All teams
                </button>
                {teamOptions.map((team) => (
                  <button
                    key={team}
                    className={`team-chip ${
                      predictionTeam === team ? "active" : ""
                    }`}
                    onClick={() => setPredictionTeam(team)}
                  >
                    {team}
                  </button>
                ))}
              </div>
            </div>
            {activePrediction.state.error && (
              <div className="alert alert-danger text-white" role="alert">
                <strong>Error:</strong> {activePrediction.state.error}
              </div>
            )}
            {activePrediction.state.loading && !activePrediction.state.data && (
              <div className="text-center py-5">
                <div className="spinner-border text-primary" role="status">
                  <span className="visually-hidden">Loading...</span>
                </div>
                <p className="text-secondary mt-3">Loading predictions...</p>
              </div>
            )}
            {activePrediction.state.data && (
              <PredictionsGrid
                predictions={[...filteredPredictions].sort((a, b) => {
                  if (predictionSort === "pred_value_asc") {
                    return (a.pred_value ?? 0) - (b.pred_value ?? 0);
                  }
                  if (predictionSort === "confidence_desc") {
                    return (b.confidence ?? 0) - (a.confidence ?? 0);
                  }
                  return (b.pred_value ?? 0) - (a.pred_value ?? 0);
                })}
                statLabel={activePrediction.label}
                unitLabel={activePrediction.unit}
              />
            )}
          </div>
        );
      default:
        return null;
    }
  };

  const jumpToTab = (tab: TabKey, loader?: () => void) => {
    setActiveTab(tab);
    if (loader) loader();
  };

  return (
    <div className="app-shell min-vh-100">
      <header className="hero-header position-relative overflow-hidden">
        <div className="hero-glow"></div>
        <div className="container position-relative">
          <nav className="navbar navbar-expand-lg navbar-dark py-4 px-0">
            <div className="d-flex align-items-center gap-2">
              <img src={logo} alt="Gamblr logo" className="brand-logo me-2" />
              <div>
                <h6 className="mb-0 text-white">Gamblr</h6>
                <span className="text-xs text-white opacity-8">NBA Analytics Hub</span>
              </div>
            </div>
            <div className="ms-auto d-flex align-items-center gap-2">
              <span className="badge badge-sm bg-gradient-success">Live</span>
              <button
                className="btn btn-sm bg-white text-dark mb-0"
                onClick={() => jumpToTab("predictions", handleLoadPredictions)}
              >
                <i className="material-symbols-rounded me-2">auto_graph</i>
                Run Models
              </button>
            </div>
          </nav>

          <div className="row align-items-center">
            <div className="col-lg-7">
              <div className="hero-copy">
                <h1 className="display-4 text-white mb-3">
                  Prediction command center for NBA props.
                </h1>
                <p className="lead text-white opacity-8 mb-4">
                  Focused points, assists, and rebounds forecasts with matchup-ready context.
                </p>
                <div className="d-flex flex-wrap gap-2">
                  <button
                    className="btn bg-gradient-primary mb-0"
                    onClick={() => jumpToTab("predictions", handleLoadPredictions)}
                  >
                    <i className="material-symbols-rounded me-2">emoji_events</i>
                    View Predictions
                  </button>
                  <button
                    className="btn btn-outline-white mb-0"
                    onClick={() => jumpToTab("props")}
                  >
                    <i className="material-symbols-rounded me-2">casino</i>
                    View Props
                  </button>
                </div>
                <div className="hero-tags mt-4">
                  <span className="badge badge-sm bg-white text-dark">Realtime APIs</span>
                  <span className="badge badge-sm bg-white text-dark">Sharp UI</span>
                  <span className="badge badge-sm bg-white text-dark">Pro insights</span>
                </div>
              </div>
            </div>
            <div className="col-lg-5 mt-4 mt-lg-0">
              <div className="hero-cards">
                <div className="card glass-card mb-3">
                  <div className="card-body">
                    <div className="d-flex align-items-center justify-content-between mb-3">
                      <div>
                        <p className="text-sm text-white opacity-8 mb-1">Confidence Index</p>
                        <h3 className="text-white mb-0">89.4</h3>
                      </div>
                      <span className="icon-shape icon-lg bg-gradient-primary shadow-primary text-white">
                        <i className="material-symbols-rounded">psychology</i>
                      </span>
                    </div>
                    <p className="text-sm text-white opacity-7 mb-0">
                      Model outputs tuned to recent form and matchup context.
                    </p>
                  </div>
                </div>
                <div className="card glass-card">
                  <div className="card-body">
                    <div className="d-flex align-items-center justify-content-between mb-3">
                      <div>
                        <p className="text-sm text-white opacity-8 mb-1">Markets Tracked</p>
                        <h3 className="text-white mb-0">1,240+</h3>
                      </div>
                      <span className="icon-shape icon-lg bg-gradient-info shadow-info text-white">
                        <i className="material-symbols-rounded">track_changes</i>
                      </span>
                    </div>
                    <p className="text-sm text-white opacity-7 mb-0">
                      Player props and game totals refreshed on demand.
                    </p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </header>

      <section className="dashboard-section py-5">
        <div className="container">
          <div className="row g-4">
            <div className="col-lg-8">
              <div className="section-card mb-4">
                <div className="section-header d-flex flex-wrap align-items-center justify-content-between">
                  <div>
                    <h3 className="mb-1">Performance Hub</h3>
                    <p className="text-sm text-secondary mb-0">
                      Filtered views of league leaders and market context.
                    </p>
                  </div>
                  <div className="d-flex flex-wrap gap-2">
                    <button
                      className="btn btn-sm bg-gradient-primary mb-0"
                      onClick={() => jumpToTab("recent", handleLoadRecent)}
                    >
                      <i className="material-symbols-rounded me-2">trending_up</i>
                      Recent Form
                    </button>
                    <button
                      className="btn btn-sm btn-outline-dark mb-0"
                      onClick={() => jumpToTab("guards", handleLoadGuards)}
                    >
                      <i className="material-symbols-rounded me-2">person</i>
                      Guard Lens
                    </button>
                  </div>
                </div>
                <div className="nav-wrapper position-relative mt-4">
                  <ul className="nav nav-pills nav-fill flex-row p-1 tab-pills" role="tablist">
                    <li className="nav-item">
                      <a
                        className={`nav-link mb-0 px-0 py-1 ${activeTab === "top_scorers" ? "active" : ""}`}
                        onClick={() => setActiveTab("top_scorers")}
                        role="tab"
                        style={{ cursor: "pointer" }}
                      >
                        <i className="material-symbols-rounded me-2">emoji_events</i>
                        Top Scorers
                      </a>
                    </li>
                    <li className="nav-item">
                      <a
                        className={`nav-link mb-0 px-0 py-1 ${activeTab === "top_assists" ? "active" : ""}`}
                        onClick={() => setActiveTab("top_assists")}
                        role="tab"
                        style={{ cursor: "pointer" }}
                      >
                        <i className="material-symbols-rounded me-2">handshake</i>
                        Assists
                      </a>
                    </li>
                    <li className="nav-item">
                      <a
                        className={`nav-link mb-0 px-0 py-1 ${activeTab === "top_rebounders" ? "active" : ""}`}
                        onClick={() => setActiveTab("top_rebounders")}
                        role="tab"
                        style={{ cursor: "pointer" }}
                      >
                        <i className="material-symbols-rounded me-2">sports_basketball</i>
                        Rebounds
                      </a>
                    </li>
                    <li className="nav-item">
                      <a
                        className={`nav-link mb-0 px-0 py-1 ${activeTab === "guards" ? "active" : ""}`}
                        onClick={() => setActiveTab("guards")}
                        role="tab"
                        style={{ cursor: "pointer" }}
                      >
                        <i className="material-symbols-rounded me-2">person</i>
                        Guards
                      </a>
                    </li>
                    <li className="nav-item">
                      <a
                        className={`nav-link mb-0 px-0 py-1 ${activeTab === "recent" ? "active" : ""}`}
                        onClick={() => setActiveTab("recent")}
                        role="tab"
                        style={{ cursor: "pointer" }}
                      >
                        <i className="material-symbols-rounded me-2">trending_up</i>
                        Recent
                      </a>
                    </li>
                    <li className="nav-item">
                      <a
                        className={`nav-link mb-0 px-0 py-1 ${activeTab === "props" ? "active" : ""}`}
                        onClick={() => setActiveTab("props")}
                        role="tab"
                        style={{ cursor: "pointer" }}
                      >
                        <i className="material-symbols-rounded me-2">casino</i>
                        Player Props
                      </a>
                    </li>
                    <li className="nav-item">
                      <a
                        className={`nav-link mb-0 px-0 py-1 ${activeTab === "predictions" ? "active" : ""}`}
                        onClick={() => setActiveTab("predictions")}
                        role="tab"
                        style={{ cursor: "pointer" }}
                      >
                        <i className="material-symbols-rounded me-2">psychology</i>
                        Predictions
                      </a>
                    </li>
                  </ul>
                </div>
              </div>

              <main>{renderContent()}</main>
            </div>
            <div className="col-lg-4">
              <div className="card shadow-lg border-radius-xl mb-4">
                <div className="card-header pb-0">
                  <h5 className="mb-0">Quick Actions</h5>
                </div>
                <div className="card-body">
                  <button
                    className="btn btn-sm bg-gradient-primary w-100 mb-3"
                    onClick={handleLoadTopScorers}
                  >
                    Refresh Top Scorers
                  </button>
                  <button
                    className="btn btn-sm btn-outline-dark w-100 mb-3"
                    onClick={handleLoadProps}
                  >
                    Fetch Player Props
                  </button>
                  <button
                    className="btn btn-sm btn-outline-dark w-100"
                    onClick={handleLoadPredictions}
                  >
                    Update Predictions
                  </button>
                </div>
              </div>
              <div className="card shadow-lg border-radius-xl mb-4">
                <div className="card-header pb-0">
                  <h5 className="mb-0">Model Notes</h5>
                </div>
                <div className="card-body">
                  <p className="text-sm text-secondary mb-3">
                    Blend matchup pace, recent usage rate, and on/off defensive data.
                  </p>
                  <div className="d-flex align-items-center justify-content-between">
                    <span className="text-sm text-secondary">Next refresh</span>
                    <span className="badge badge-sm bg-gradient-info">Every 5 min</span>
                  </div>
                </div>
              </div>
              <div className="card shadow-lg border-radius-xl">
                <div className="card-header pb-0">
                  <h5 className="mb-0">Focus Checklist</h5>
                </div>
                <div className="card-body">
                  <div className="d-flex align-items-start mb-3">
                    <span className="icon-shape icon-xs bg-gradient-success me-2">
                      <i className="material-symbols-rounded">check</i>
                    </span>
                    <p className="text-sm text-secondary mb-0">Compare pace vs. opponent.</p>
                  </div>
                  <div className="d-flex align-items-start mb-3">
                    <span className="icon-shape icon-xs bg-gradient-warning me-2">
                      <i className="material-symbols-rounded">schedule</i>
                    </span>
                    <p className="text-sm text-secondary mb-0">Monitor minutes volatility.</p>
                  </div>
                  <div className="d-flex align-items-start">
                    <span className="icon-shape icon-xs bg-gradient-info me-2">
                      <i className="material-symbols-rounded">insights</i>
                    </span>
                    <p className="text-sm text-secondary mb-0">Track books with best lines.</p>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <footer className="footer py-5 mt-5">
            <div className="container">
              <div className="row">
                <div className="col-8 mx-auto text-center mt-1">
                  <p className="mb-0 text-secondary">
                    Powered by FastAPI and React - Built with Material Kit
                  </p>
                </div>
              </div>
            </div>
          </footer>
        </div>
      </section>
    </div>
  );
}

export default App;
