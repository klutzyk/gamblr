import { useEffect, useState } from "react";
import "./App.css";
import logo from "./assets/logo2.png";
import {
  getTopScorers,
  getTopAssists,
  getTopRebounders,
  getGuardStats,
  getRecentPerformers,
  getNbaEvents,
  getOddsUsageSnapshot,
  getOddsEventProps,
  getPointsPredictions,
  getAssistsPredictions,
  getReboundsPredictions,
  getThreeptPredictions,
  getThreepaPredictions,
  getFirstBasketPredictions,
  getDoublesPredictions,
  getBestBets,
  getBestBetsProgress,
  syncPlayerPropsWindow,
  getApiHealth,
  type PlayerRow,
  type OddsEvent,
  type OddsEventPropsResponse,
  type OddsUsageSnapshot,
  type PredictionRow,
  type FirstBasketPredictionRow,
  type DoubleTriplePredictionRow,
  type BestBetsResponse,
  type BestBetsProgress,
  type PredictionDayParam,
} from "./api";

type NormalizedPropRow = {
  eventId: string;
  matchup: string;
  player: string;
  marketKey: string;
  outcomeType: string;
  line: number | null;
  odds: number | null;
  sportsbook: string;
  lastUpdate: string | null;
};

type TabKey =
  | "top_scorers"
  | "top_assists"
  | "top_rebounders"
  | "guards"
  | "recent"
  | "props"
  | "matchups"
  | "predictions"
  | "best_bets"
  | "first_basket"
  | "double_triple";

type UserRegion = "au" | "us" | "uk";

const REGION_CONFIG: Record<
  UserRegion,
  { label: string; locale: string; timeZone: string; short: string }
> = {
  au: {
    label: "Australia",
    locale: "en-AU",
    timeZone: "Australia/Sydney",
    short: "AET",
  },
  us: {
    label: "USA",
    locale: "en-US",
    timeZone: "America/New_York",
    short: "ET",
  },
  uk: {
    label: "England",
    locale: "en-GB",
    timeZone: "Europe/London",
    short: "UK",
  },
};

const TEAM_NAME_TO_ABBR: Record<string, string> = {
  "atlanta hawks": "ATL",
  "boston celtics": "BOS",
  "brooklyn nets": "BKN",
  "charlotte hornets": "CHA",
  "chicago bulls": "CHI",
  "cleveland cavaliers": "CLE",
  "dallas mavericks": "DAL",
  "denver nuggets": "DEN",
  "detroit pistons": "DET",
  "golden state warriors": "GSW",
  "houston rockets": "HOU",
  "indiana pacers": "IND",
  "los angeles clippers": "LAC",
  "la clippers": "LAC",
  "los angeles lakers": "LAL",
  "la lakers": "LAL",
  "memphis grizzlies": "MEM",
  "miami heat": "MIA",
  "milwaukee bucks": "MIL",
  "minnesota timberwolves": "MIN",
  "new orleans pelicans": "NOP",
  "new york knicks": "NYK",
  "ny knicks": "NYK",
  "oklahoma city thunder": "OKC",
  "orlando magic": "ORL",
  "philadelphia 76ers": "PHI",
  "phoenix suns": "PHX",
  "portland trail blazers": "POR",
  "sacramento kings": "SAC",
  "san antonio spurs": "SAS",
  "toronto raptors": "TOR",
  "utah jazz": "UTA",
  "washington wizards": "WAS",
};

function normalizeTeamToAbbr(teamName: string) {
  const trimmed = (teamName ?? "").trim();
  if (!trimmed) return trimmed;
  const cleaned = trimmed.toLowerCase().replace(/\./g, "");
  if (TEAM_NAME_TO_ABBR[cleaned]) return TEAM_NAME_TO_ABBR[cleaned];
  if (/^[a-z]{2,4}$/.test(cleaned)) return cleaned.toUpperCase();
  return trimmed;
}

function normalizeMatchupKey(raw: string) {
  if (!raw) return "";
  const cleaned = raw
    .replace(/\s+/g, " ")
    .replace(/vs\.?/gi, "@")
    .replace(/v\.?/gi, "@")
    .trim();
  const parts = cleaned.split("@").map((part) => part.trim()).filter(Boolean);
  if (parts.length >= 2) {
    const away = normalizeTeamToAbbr(parts[0]);
    const home = normalizeTeamToAbbr(parts[1]);
    return `${away}@${home}`;
  }
  return normalizeTeamToAbbr(cleaned);
}

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

function getPlayerStatsSearchUrl(playerName: string | null | undefined): string {
  const safeName =
    typeof playerName === "string" && playerName.trim().length > 0
      ? playerName.trim()
      : "NBA player";
  const url = new URL("https://www.google.com/search");
  url.searchParams.set("q", `${safeName} stats`);
  return url.toString();
}

function normalizeOddsEventProps(payload: OddsEventPropsResponse["data"]): NormalizedPropRow[] {
  const rows: NormalizedPropRow[] = [];
  for (const book of payload.bookmakers ?? []) {
    for (const market of book.markets ?? []) {
      for (const outcome of market.outcomes ?? []) {
        rows.push({
          eventId: payload.id,
          matchup: `${payload.away_team} @ ${payload.home_team}`,
          player: outcome.description,
          marketKey: market.key,
          outcomeType: outcome.name,
          line: typeof outcome.point === "number" ? outcome.point : null,
          odds: typeof outcome.price === "number" ? outcome.price : null,
          sportsbook: book.title,
          lastUpdate: market.last_update ?? null,
        });
      }
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
                    <h6 className="mb-0 text-sm">
                      <a
                        href={getPlayerStatsSearchUrl(row.PLAYER_NAME)}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="player-name-link"
                      >
                        {row.PLAYER_NAME}
                      </a>
                    </h6>
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
  statKey,
  formatGameDate,
}: {
  predictions: PredictionRow[];
  statLabel: string;
  unitLabel: string;
  statKey: "points" | "assists" | "rebounds" | "threept" | "threepa";
  formatGameDate: (dateString: string) => string;
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

  const getInitials = (name: string | null | undefined) =>
    (name ?? "")
      .split(" ")
      .filter(Boolean)
      .slice(0, 2)
      .map((part) => part[0])
      .join("")
      .toUpperCase() || "NA";

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
        const underRiskValue =
          typeof pred.under_risk === "number" ? pred.under_risk : null;
        let underRiskClass = "risk-unknown";
        if (underRiskValue !== null) {
          if (underRiskValue >= 0.6) underRiskClass = "risk-high";
          else if (underRiskValue >= 0.35) underRiskClass = "risk-mid";
          else underRiskClass = "risk-low";
        }
        const underRiskPct =
          underRiskValue !== null ? `${(underRiskValue * 100).toFixed(0)}%` : "n/a";
        const underThresholdValue =
          statKey === "points"
            ? typeof pred.pred_p10 === "number" && typeof pred.pred_value === "number"
              ? (pred.pred_p10 + pred.pred_value) / 2
              : pred.pred_value
            : typeof pred.pred_p10 === "number"
              ? pred.pred_p10
              : null;
        const underThresholdText =
          typeof underThresholdValue === "number"
            ? `${underThresholdValue.toFixed(1)} ${unitLabel}`
            : "n/a";
        const lastUnderText =
          typeof pred.last_under_value === "number"
            ? `${pred.last_under_value.toFixed(1)} ${unitLabel}`
            : "n/a";
        const lastUnderMeta =
          typeof pred.last_under_games_ago === "number"
            ? `${pred.last_under_games_ago}g ago`
            : null;
        const lastUnderMatchup =
          typeof pred.last_under_matchup === "string" && pred.last_under_matchup.length
            ? pred.last_under_matchup
            : null;
        const lastUnderMinutes =
          typeof pred.last_under_minutes === "number"
            ? `${pred.last_under_minutes.toFixed(0)} min`
            : null;
        const lastUnderBits = [
          lastUnderMeta,
          lastUnderMatchup,
          lastUnderText !== "n/a" ? lastUnderText : null,
          lastUnderMinutes,
        ].filter(Boolean);
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
                  <h5 className="mb-1">
                    <a
                      href={getPlayerStatsSearchUrl(pred.full_name)}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="player-name-link"
                    >
                      {pred.full_name}
                    </a>
                  </h5>
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
              <span className="text-sm text-secondary">{formatGameDate(pred.game_date)}</span>
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
            <div className={`under-risk-card mt-3 ${underRiskClass}`}>
              <div className="under-risk-row">
                <span className="label">
                  Under <strong className="under-risk-x">{underThresholdText}</strong> next game
                </span>
                <span className="value">{underRiskPct}</span>
              </div>
              {lastUnderMeta && (
                <div className="under-risk-meta">
                  <span className="under-risk-last">
                    Last under: {lastUnderBits.join(" • ")}
                  </span>
                </div>
              )}
              <div className="under-risk-bar">
                <div
                  className="under-risk-fill"
                  style={{
                    width:
                      underRiskValue !== null
                        ? `${Math.min(100, Math.max(0, underRiskValue * 100))}%`
                        : "0%",
                  }}
                ></div>
              </div>
            </div>
          </div>
        </div>
        );
      })}
    </div>
  );
}

function FirstBasketGrid({
  rows,
  userRegion,
}: {
  rows: FirstBasketPredictionRow[];
  userRegion: UserRegion;
}) {
  if (!rows.length) {
    return (
      <div className="text-center py-5">
        <p className="text-secondary">No first basket predictions available.</p>
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
            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Matchup</th>
            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Tipoff</th>
            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">First Basket %</th>
            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Team First %</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={`${row.game_id ?? row.matchup}-${row.player_id}`}>
              <td className="text-sm">
                <a
                  href={getPlayerStatsSearchUrl(row.full_name)}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="player-name-link"
                >
                  {row.full_name}
                </a>
              </td>
              <td className="text-sm">{row.team_abbreviation}</td>
              <td className="text-sm">{row.matchup}</td>
              <td className="text-sm">
                {userRegion === "au"
                  ? row.tipoff_au ?? row.tipoff_et ?? "-"
                  : row.tipoff_et ?? row.tipoff_au ?? "-"}
              </td>
              <td className="text-sm text-primary font-weight-bold">
                {`${(row.first_basket_prob * 100).toFixed(1)}%`}
              </td>
              <td className="text-sm">
                {typeof row.team_scores_first_prob === "number"
                  ? `${(row.team_scores_first_prob * 100).toFixed(1)}%`
                  : "-"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function DoubleTripleGrid({ rows }: { rows: DoubleTriplePredictionRow[] }) {
  if (!rows.length) {
    return (
      <div className="text-center py-5">
        <p className="text-secondary">No double-double or triple-double data available.</p>
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
            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Matchup</th>
            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Proj (P/R/A)</th>
            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Double-Double</th>
            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Triple-Double</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={`${row.game_id ?? row.matchup}-${row.player_id}`}>
              <td className="text-sm">
                <a
                  href={getPlayerStatsSearchUrl(row.full_name)}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="player-name-link"
                >
                  {row.full_name}
                </a>
              </td>
              <td className="text-sm">{row.team_abbreviation}</td>
              <td className="text-sm">{row.matchup}</td>
              <td className="text-sm">
                {`${formatNumber(row.pts_pred)} / ${formatNumber(row.reb_pred)} / ${formatNumber(row.ast_pred)}`}
              </td>
              <td className="text-sm text-primary font-weight-bold">
                {typeof row.double_double_prob === "number"
                  ? `${(row.double_double_prob * 100).toFixed(1)}%`
                  : "-"}
              </td>
              <td className="text-sm text-success font-weight-bold">
                {typeof row.triple_double_prob === "number"
                  ? `${(row.triple_double_prob * 100).toFixed(1)}%`
                  : "-"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function App() {
  const [activeTab, setActiveTab] = useState<TabKey>("predictions");
  const [userRegion, setUserRegion] = useState<UserRegion>("au");

  const [topScorers, setTopScorers] =
    useState<ApiState<PlayerRow[]>>(initialState);
  const [topAssists, setTopAssists] =
    useState<ApiState<PlayerRow[]>>(initialState);
  const [topRebounders, setTopRebounders] =
    useState<ApiState<PlayerRow[]>>(initialState);
  const [guards, setGuards] = useState<ApiState<PlayerRow[]>>(initialState);
  const [recent, setRecent] = useState<ApiState<PlayerRow[]>>(initialState);
  const [eventsState, setEventsState] =
    useState<ApiState<OddsEvent[]>>(initialState);
  const [propsState, setPropsState] =
    useState<ApiState<OddsEventPropsResponse>>(initialState);
  const [selectedEventId, setSelectedEventId] = useState("");
  const [predictionStat, setPredictionStat] = useState<
    "points" | "assists" | "rebounds" | "threept" | "threepa"
  >("points");
  const [pointsPredictionsState, setPointsPredictionsState] =
    useState<ApiState<PredictionRow[]>>(initialState);
  const [assistsPredictionsState, setAssistsPredictionsState] =
    useState<ApiState<PredictionRow[]>>(initialState);
  const [reboundsPredictionsState, setReboundsPredictionsState] =
    useState<ApiState<PredictionRow[]>>(initialState);
  const [threeptPredictionsState, setThreeptPredictionsState] =
    useState<ApiState<PredictionRow[]>>(initialState);
  const [threepaPredictionsState, setThreepaPredictionsState] =
    useState<ApiState<PredictionRow[]>>(initialState);
  const [firstBasketPredictionsState, setFirstBasketPredictionsState] =
    useState<ApiState<FirstBasketPredictionRow[]>>(initialState);
  const [doubleTripleState, setDoubleTripleState] =
    useState<ApiState<DoubleTriplePredictionRow[]>>(initialState);
  const [bestBetsState, setBestBetsState] =
    useState<ApiState<BestBetsResponse>>(initialState);
  const [predictionDay, setPredictionDay] = useState<
    "today" | "tomorrow" | "yesterday" | "auto"
  >("auto");
  const [predictionSort, setPredictionSort] = useState<
    "pred_value_desc" | "pred_value_asc" | "confidence_desc"
  >("pred_value_desc");
  const [underRiskSort, setUnderRiskSort] = useState<
    "none" | "under_risk_desc" | "under_risk_asc"
  >("none");
  const [predictionSearch, setPredictionSearch] = useState("");
  const [predictionTeams, setPredictionTeams] = useState<string[]>([]);
  const [predictionLine, setPredictionLine] = useState<number | "all">("all");
  const [selectedPredictionMatchup, setSelectedPredictionMatchup] = useState("");
  const [firstBasketSort, setFirstBasketSort] = useState<
    "prob_desc" | "prob_asc"
  >("prob_desc");
  const [targetMultiplierInput, setTargetMultiplierInput] = useState("2");
  const [bestBetLegCount, setBestBetLegCount] = useState(2);
  const [wildcardLegs, setWildcardLegs] = useState(false);
  const [bestBetSyncMode, setBestBetSyncMode] = useState<
    "auto" | "night" | "morning" | "all"
  >("auto");
  const bestBetEvents = 4;
  const [includeComboMarkets, setIncludeComboMarkets] = useState(false);
  const [selectedBestBetEventIds, setSelectedBestBetEventIds] = useState<string[]>([]);
  const [syncSummary, setSyncSummary] = useState<string | null>(null);
  const [isSyncingBestBets, setIsSyncingBestBets] = useState(false);
  const [oddsUsage, setOddsUsage] = useState<ApiState<OddsUsageSnapshot>>(initialState);
  const [bestBetsProgress, setBestBetsProgress] =
    useState<ApiState<BestBetsProgress>>(initialState);
  const [bestBetLoadingPhase, setBestBetLoadingPhase] = useState<
    "idle" | "syncing" | "scoring" | "ranking"
  >("idle");
  const [bestBetLoadingStartedAt, setBestBetLoadingStartedAt] = useState<number | null>(null);
  const [bestBetLoadingTick, setBestBetLoadingTick] = useState(0);
  const [backendHealth, setBackendHealth] = useState<
    "checking" | "up" | "down"
  >("checking");
  const [backendCheckedAt, setBackendCheckedAt] = useState<number | null>(null);
  const [backendError, setBackendError] = useState<string | null>(null);
  const regionConfig = REGION_CONFIG[userRegion];
  const dayLabelSuffix =
    userRegion === "us" ? "(ET)" : `(${regionConfig.short})`;
  const dayOptions: Array<{
    value: "auto" | "today" | "tomorrow" | "yesterday";
    label: string;
  }> = [
    { value: "auto", label: `Auto ${dayLabelSuffix}` },
    { value: "today", label: `Today ${dayLabelSuffix}` },
    { value: "tomorrow", label: `Tomorrow ${dayLabelSuffix}` },
    { value: "yesterday", label: `Yesterday ${dayLabelSuffix}` },
  ];

  const toUtcMidnightMs = (parts: { year: number; month: number; day: number }) =>
    Date.UTC(parts.year, parts.month - 1, parts.day);

  const getDatePartsInTz = (date: Date, timeZone: string) => {
    const formatter = new Intl.DateTimeFormat("en-US", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    });
    const parts = formatter.formatToParts(date);
    const year = Number(parts.find((p) => p.type === "year")?.value);
    const month = Number(parts.find((p) => p.type === "month")?.value);
    const day = Number(parts.find((p) => p.type === "day")?.value);
    return { year, month, day };
  };

  const mapDayToEtForApi = (
    selectedDay: "today" | "tomorrow" | "yesterday" | "auto"
  ): PredictionDayParam => {
    const selectedOffset: Record<"today" | "tomorrow" | "yesterday", number> = {
      yesterday: -1,
      today: 0,
      tomorrow: 1,
    };

    const now = new Date();
    const etToday = getDatePartsInTz(now, "America/New_York");
    const userToday = getDatePartsInTz(now, regionConfig.timeZone);
    const userEtDeltaDays = Math.round(
      (toUtcMidnightMs(userToday) - toUtcMidnightMs(etToday)) / (24 * 60 * 60 * 1000)
    );

    // Australia needs two modes:
    // - Morning AU: local calendar day is ahead of ET, but the relevant NBA slate is still ET "today".
    // - Later AU day: ET has caught up, so local "today" maps back one ET day.
    if (userRegion === "au") {
      const auMapsDirectlyToCurrentEtSlate = userEtDeltaDays >= 1;

      if (selectedDay === "auto") {
        return auMapsDirectlyToCurrentEtSlate ? "today" : "yesterday";
      }

      if (auMapsDirectlyToCurrentEtSlate) {
        if (selectedDay === "yesterday") return "yesterday";
        if (selectedDay === "today") return "today";
        return "tomorrow";
      }

      if (selectedDay === "yesterday") return "two_days_ago";
      if (selectedDay === "today") return "yesterday";
      return "today";
    }

    if (selectedDay === "auto") return "auto";

    const etOffset = selectedOffset[selectedDay] - userEtDeltaDays;

    if (etOffset <= -2) return "two_days_ago";
    if (etOffset === -1) return "yesterday";
    if (etOffset === 0) return "today";
    if (etOffset >= 1) return "tomorrow";
    return "auto";
  };

  const predictionApiDay = mapDayToEtForApi(predictionDay);

  const formatDateByRegion = (value: string | Date | number) =>
    new Date(value).toLocaleDateString(regionConfig.locale, {
      timeZone: regionConfig.timeZone,
      month: "short",
      day: "numeric",
      year: "numeric",
    });

  const formatSlateDateForRegion = (value: string) => {
    // `game_date` is an NBA slate date in ET (date-only).
    // For AU/UK users, that slate usually lands on the next local calendar day.
    const normalized = String(value).slice(0, 10);
    const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(normalized);
    if (!match) return formatDateByRegion(value);
    const year = Number(match[1]);
    const month = Number(match[2]) - 1;
    const day = Number(match[3]);
    const baseUtc = new Date(Date.UTC(year, month, day));
    const dayShift = userRegion === "us" ? 0 : 1;
    baseUtc.setUTCDate(baseUtc.getUTCDate() + dayShift);
    return baseUtc.toLocaleDateString(regionConfig.locale, {
      timeZone: "UTC",
      month: "short",
      day: "numeric",
      year: "numeric",
    });
  };

  const formatMatchupKickoffLabel = (
    value: string | Date | number,
    includeZoneSuffix = userRegion === "au"
  ) => {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return "Date tbd";
    const parts = new Intl.DateTimeFormat("en-GB", {
      timeZone: regionConfig.timeZone,
      day: "numeric",
      month: "short",
      hour: "2-digit",
      minute: "2-digit",
      hour12: true,
    }).formatToParts(date);
    const day = parts.find((part) => part.type === "day")?.value ?? "";
    const month = parts.find((part) => part.type === "month")?.value ?? "";
    const hour = parts.find((part) => part.type === "hour")?.value ?? "";
    const minute = parts.find((part) => part.type === "minute")?.value ?? "";
    const period = (parts.find((part) => part.type === "dayPeriod")?.value ?? "").toLowerCase();
    const zoneSuffix = includeZoneSuffix
      ? ` ${userRegion === "au" ? "AEST" : regionConfig.short}`
      : "";
    return `${day} ${month}, ${hour}:${minute} ${period}${zoneSuffix}`;
  };

  const formatPredictionMatchupKickoff = (row: PredictionRow) => {
    if (userRegion === "au" && row.tipoff_au) {
      const parsedAu = new Date(row.tipoff_au);
      if (!Number.isNaN(parsedAu.getTime())) {
        return formatMatchupKickoffLabel(parsedAu, true);
      }
      return row.tipoff_au;
    }
    if (row.tipoff_et && row.game_date) {
      const parsed = new Date(`${row.game_date}T00:00:00`);
      if (!Number.isNaN(parsed.getTime())) {
        return `${formatSlateDateForRegion(row.game_date)}${userRegion === "au" ? " AEST" : ""}`;
      }
    }
    return row.game_date ? formatSlateDateForRegion(row.game_date) : "Date tbd";
  };

  const formatDateTimeByRegion = (value: string | Date | number) =>
    formatMatchupKickoffLabel(value, userRegion === "au");

  const formatTimeByRegion = (value: string | Date | number) =>
    new Date(value).toLocaleTimeString(regionConfig.locale, {
      timeZone: regionConfig.timeZone,
      hour: "2-digit",
      minute: "2-digit",
    });

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

  const handleLoadEvents = (force = false) =>
    safeLoad(
      eventsState,
      setEventsState,
      () => getNbaEvents(),
      5 * 60 * 1000,
      force
    );

  const handleLoadProps = () => {
    if (!selectedEventId) {
      setPropsState({
        ...propsState,
        error: "Select a matchup first.",
      });
      return;
    }
    return safeLoad(
      propsState,
      setPropsState,
      () => getOddsEventProps(selectedEventId),
      5 * 60 * 1000
    );
  };

  const predictionConfig = {
    points: {
      label: "Points",
      unit: "pts",
      state: pointsPredictionsState,
      setState: setPointsPredictionsState,
      loader: () => getPointsPredictions(predictionApiDay),
    },
    assists: {
      label: "Assists",
      unit: "ast",
      state: assistsPredictionsState,
      setState: setAssistsPredictionsState,
      loader: () => getAssistsPredictions(predictionApiDay),
    },
    rebounds: {
      label: "Rebounds",
      unit: "reb",
      state: reboundsPredictionsState,
      setState: setReboundsPredictionsState,
      loader: () => getReboundsPredictions(predictionApiDay),
    },
    threept: {
      label: "3PT Made",
      unit: "3PM",
      state: threeptPredictionsState,
      setState: setThreeptPredictionsState,
      loader: () => getThreeptPredictions(predictionApiDay),
    },
    threepa: {
      label: "3PT Attempts",
      unit: "3PA",
      state: threepaPredictionsState,
      setState: setThreepaPredictionsState,
      loader: () => getThreepaPredictions(predictionApiDay),
    },
  };

  const bestBetMatchupOptions = (eventsState.data ?? []).map((event) => ({
    id: event.id,
    label: `${event.away_team} @ ${event.home_team}`,
    kickoff: formatDateTimeByRegion(event.commence_time),
  }));
  const activeBestBetMatchups =
    selectedBestBetEventIds.length > 0
      ? bestBetMatchupOptions.filter((event) => selectedBestBetEventIds.includes(event.id))
      : bestBetMatchupOptions.slice(0, bestBetEvents);
  const bestBetFocusLabel =
    activeBestBetMatchups.length > 0
      ? activeBestBetMatchups[bestBetLoadingTick % activeBestBetMatchups.length]
      : null;
  const bestBetsProgressData = bestBetsProgress.data;
  const bestBetsProgressMatchup = bestBetsProgressData?.current_matchup
    ? activeBestBetMatchups.find(
        (event) => normalizeMatchupKey(event.label) === normalizeMatchupKey(bestBetsProgressData.current_matchup ?? "")
      ) ?? {
        id: "",
        label: bestBetsProgressData.current_matchup,
        kickoff: "",
      }
    : null;
  const bestBetsProgressPhase = bestBetsProgressData?.phase ?? null;
  const bestBetElapsedSeconds = bestBetLoadingStartedAt
    ? Math.max(0, Math.round((Date.now() - bestBetLoadingStartedAt) / 1000))
    : 0;
  const bestBetsProgressSummaryParts = [
    (bestBetsProgressPhase === "scoring" || bestBetsProgressPhase === "ranking") &&
    typeof bestBetsProgressData?.rows_processed === "number" &&
    typeof bestBetsProgressData?.rows_total === "number" &&
    bestBetsProgressData.rows_total > 0
      ? `${bestBetsProgressData.rows_processed}/${bestBetsProgressData.rows_total} props`
      : null,
    (bestBetsProgressPhase === "scoring" || bestBetsProgressPhase === "ranking") &&
    typeof bestBetsProgressData?.candidates_kept === "number" &&
    bestBetsProgressData.candidates_kept > 0
      ? `${bestBetsProgressData.candidates_kept} candidates`
      : null,
    bestBetsProgressPhase === "ranking" &&
    typeof bestBetsProgressData?.combos_considered === "number" &&
    bestBetsProgressData.combos_considered > 0
      ? `${bestBetsProgressData.combos_considered} combos`
      : null,
  ].filter(Boolean) as string[];
  const oddsCreditsRemaining =
    typeof oddsUsage.data?.requests_remaining === "number"
      ? oddsUsage.data.requests_remaining
      : null;

  const currentPredictionRows = predictionConfig[predictionStat].state.data ?? [];
  const predictionMatchupMap = currentPredictionRows.reduce(
    (map, row) => {
      const matchup = row.matchup;
      if (!matchup) return map;
      const matchupKey = normalizeMatchupKey(matchup);
      if (!map.has(matchupKey)) {
        map.set(matchupKey, {
          key: matchupKey,
          label: matchup,
          kickoff: formatPredictionMatchupKickoff(row),
        });
      }
      return map;
    },
    new Map<string, { key: string; label: string; kickoff: string }>()
  );

  const eventMatchupMap = new Map(
    bestBetMatchupOptions.map((event) => {
      const key = normalizeMatchupKey(event.label);
      return [
        key,
        {
          key,
          label: event.label,
          kickoff: event.kickoff,
        },
      ] as const;
    })
  );

  const currentPredictionMatchups = Array.from(
    predictionMatchupMap.entries(),
    ([key, fallback]) => eventMatchupMap.get(key) ?? fallback
  ).sort((a, b) => a.label.localeCompare(b.label));

  const selectedPredictionMatchupOption =
    currentPredictionMatchups.find((item) => item.key === selectedPredictionMatchup) ?? null;

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

  const handleLoadBestBets = async (force = false) => {
    const targetMultiplier = Number(targetMultiplierInput);
    if (!targetMultiplier || Number.isNaN(targetMultiplier) || targetMultiplier <= 1) {
      setBestBetsState({
        ...bestBetsState,
        error: "Target multiplier must be a number greater than 1 (e.g. 2).",
      });
      return;
    }
    const getPredictionDayFromSelectedEvents = () => {
      if (!eventsState.data || selectedBestBetEventIds.length === 0) return null;
      const selected = eventsState.data.filter((event) =>
        selectedBestBetEventIds.includes(event.id)
      );
      if (selected.length === 0) return null;
      const earliest = selected
        .map((event) => new Date(event.commence_time))
        .filter((date) => !Number.isNaN(date.getTime()))
        .sort((a, b) => a.getTime() - b.getTime())[0];
      if (!earliest) return null;
      const tz = "America/New_York";
      const formatKey = (date: Date) =>
        new Intl.DateTimeFormat("en-CA", {
          timeZone: tz,
          year: "numeric",
          month: "2-digit",
          day: "2-digit",
        }).format(date);
      const parseKey = (key: string) => {
        const [year, month, day] = key.split("-").map(Number);
        return Date.UTC(year, month - 1, day);
      };
      const eventKey = formatKey(earliest);
      const nowKey = formatKey(new Date());
      const diffDays = Math.round(
        (parseKey(eventKey) - parseKey(nowKey)) / (24 * 60 * 60 * 1000)
      );
      // Align selected event date to ET day labels expected by prediction endpoints.
      if (diffDays === 0) return "today";
      if (diffDays === 1) return "tomorrow";
      if (diffDays === -1) return "yesterday";
      return "auto";
    };

    const derivedPredictionDay =
      getPredictionDayFromSelectedEvents() ??
      (bestBetSyncMode === "night"
        ? "tomorrow"
        : bestBetSyncMode === "morning"
          ? "today"
          : "auto");

    setBestBetLoadingStartedAt(Date.now());
    setBestBetLoadingTick(0);
    setBestBetsProgress(initialState<BestBetsProgress>());
    void loadBestBetsProgress(true);

    await safeLoad(
      bestBetsState,
      setBestBetsState,
      () =>
        getBestBets({
          target_multiplier: targetMultiplier,
          leg_count: bestBetLegCount,
          leg_mode: wildcardLegs ? "up_to" : "exact",
          max_legs: wildcardLegs ? bestBetLegCount : undefined,
          day: derivedPredictionDay,
          bookmaker: "sportsbet",
          include_combos: includeComboMarkets,
          event_ids:
            selectedBestBetEventIds.length > 0
              ? selectedBestBetEventIds.join(",")
              : undefined,
          min_confidence: 55,
          min_edge: 0.02,
          min_prob: 0.52,
          max_candidates: 36,
        }),
      2 * 60 * 1000,
      force
    );
    void loadBestBetsProgress(true);
  };

  const handleSyncAndLoadBestBets = async () => {
    setSyncSummary(null);
    setIsSyncingBestBets(true);
    setBestBetLoadingPhase("syncing");
    setBestBetLoadingStartedAt(Date.now());
    setBestBetLoadingTick(0);
    setBestBetsProgress(initialState<BestBetsProgress>());
    void loadBestBetsProgress(true);
    try {
      const selectedMarkets = includeComboMarkets
        ? "player_points,player_assists,player_rebounds," +
          "player_points_rebounds_assists,player_points_rebounds," +
          "player_points_assists,player_rebounds_assists"
        : "player_points,player_assists,player_rebounds";
      const sync = await syncPlayerPropsWindow({
        bookmakers: "sportsbet",
        markets: selectedMarkets,
        min_remaining_after_call: 5,
        max_events: bestBetEvents,
        schedule_mode:
          selectedBestBetEventIds.length > 0 ? "auto" : bestBetSyncMode,
        event_ids:
          selectedBestBetEventIds.length > 0
            ? selectedBestBetEventIds.join(",")
            : undefined,
      });
      const processed = Number(sync.events_processed ?? 0);
      const considered = Number(sync.events_considered ?? 0);
      const usage = sync.usage as Record<string, unknown> | undefined;
      const remaining =
        usage && typeof usage.requests_remaining !== "undefined"
          ? ` | credits left: ${usage.requests_remaining}`
          : "";
      if (usage) {
        setOddsUsage({
          loading: false,
          error: null,
          data: {
            requests_last:
              typeof usage.requests_last === "number" ? usage.requests_last : null,
            requests_remaining:
              typeof usage.requests_remaining === "number"
                ? usage.requests_remaining
                : null,
            requests_used:
              typeof usage.requests_used === "number" ? usage.requests_used : null,
          },
          lastFetched: Date.now(),
        });
      }
      setSyncSummary(`Synced ${processed}/${considered} events${remaining}`);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to sync props.";
      setSyncSummary(`Sync error: ${message}`);
    } finally {
      setBestBetLoadingPhase("scoring");
      await handleLoadBestBets(true);
      setBestBetLoadingPhase("idle");
      setBestBetLoadingStartedAt(null);
      setIsSyncingBestBets(false);
    }
  };

  const handleLoadFirstBasketPredictions = (force = false) =>
    safeLoad(
      firstBasketPredictionsState,
      setFirstBasketPredictionsState,
      () => getFirstBasketPredictions(predictionApiDay, 6),
      5 * 60 * 1000,
      force
    );

  const handleLoadDoubleTriple = (force = false) =>
    safeLoad(
      doubleTripleState,
      setDoubleTripleState,
      () => getDoublesPredictions(predictionApiDay, 40),
      5 * 60 * 1000,
      force
    );

  const checkBackendHealth = async () => {
    setBackendHealth("checking");
    try {
      await getApiHealth();
      setBackendHealth("up");
      setBackendError(null);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Health check failed";
      setBackendHealth("down");
      setBackendError(
        message.includes("status")
          ? "Could not reach the backend right now."
          : "Backend is sleeping or temporarily unavailable."
      );
    } finally {
      setBackendCheckedAt(Date.now());
    }
  };

  const loadOddsUsageSnapshot = (force = false) =>
    safeLoad(
      oddsUsage,
      setOddsUsage,
      () => getOddsUsageSnapshot(),
      30 * 1000,
      force
    );

  const loadBestBetsProgress = (force = false) =>
    safeLoad(
      bestBetsProgress,
      setBestBetsProgress,
      () => getBestBetsProgress(),
      1000,
      force
    );

  useEffect(() => {
    handleLoadPredictions(true);
  }, [predictionApiDay, predictionStat]);

  useEffect(() => {
    if (currentPredictionMatchups.length === 0) {
      setSelectedPredictionMatchup("");
      return;
    }
    setSelectedPredictionMatchup((prev) =>
      currentPredictionMatchups.some((item) => item.key === prev)
        ? prev
        : currentPredictionMatchups[0].key
    );
  }, [
    predictionStat,
    predictionApiDay,
    currentPredictionMatchups.map((item) => item.key).join("|"),
  ]);

  useEffect(() => {
    if (activeTab === "first_basket") {
      void handleLoadFirstBasketPredictions(true);
    }
    if (activeTab === "double_triple") {
      void handleLoadDoubleTriple(true);
    }
  }, [activeTab, predictionApiDay]);

  useEffect(() => {
    const defaults: Record<typeof predictionStat, number> = {
      points: 5,
      assists: 2,
      rebounds: 2,
      threept: 1,
      threepa: 3,
    };
    setPredictionLine(defaults[predictionStat]);
  }, [predictionStat]);

  useEffect(() => {
    if (activeTab === "props") {
      void handleLoadEvents();
    }
    if (activeTab === "best_bets") {
      void (async () => {
        await handleLoadEvents(true);
        await loadOddsUsageSnapshot(true);
      })();
    }
    if (activeTab === "matchups") {
      void handleLoadEvents(true);
    }
  }, [activeTab]);

  useEffect(() => {
    if (!selectedEventId && eventsState.data && eventsState.data.length > 0) {
      setSelectedEventId(eventsState.data[0].id);
    }
  }, [eventsState.data, selectedEventId]);

  useEffect(() => {
    if (!eventsState.data) return;
    const validIds = new Set(eventsState.data.map((e) => e.id));
    setSelectedBestBetEventIds((prev) => prev.filter((id) => validIds.has(id)));
  }, [eventsState.data]);

  useEffect(() => {
    if (!isSyncingBestBets && !bestBetsState.loading) return;
    const interval = setInterval(() => {
      setBestBetLoadingTick((prev) => prev + 1);
      void loadBestBetsProgress(true);
    }, 1500);
    return () => clearInterval(interval);
  }, [isSyncingBestBets, bestBetsState.loading]);

  useEffect(() => {
    if (isSyncingBestBets || bestBetsState.loading) return;
    setBestBetLoadingStartedAt(null);
    setBestBetLoadingPhase("idle");
  }, [isSyncingBestBets, bestBetsState.loading]);

  useEffect(() => {
    void checkBackendHealth();
    const interval = setInterval(() => {
      void checkBackendHealth();
    }, 45_000);
    return () => clearInterval(interval);
  }, []);

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
              <h4 className="mb-1">Player Props (by Matchup)</h4>
              <p className="text-sm text-secondary mb-0">
                Select a live/upcoming matchup instead of manually entering IDs.
              </p>
            </div>
            <div className="d-flex flex-wrap gap-2 mb-4">
              <select
                className="form-select"
                value={selectedEventId}
                onChange={(e) => setSelectedEventId(e.target.value)}
                style={{ maxWidth: "420px" }}
                disabled={eventsState.loading}
              >
                {eventsState.data && eventsState.data.length > 0 ? (
                  eventsState.data.map((event) => (
                    <option key={event.id} value={event.id}>
                      {event.away_team} @ {event.home_team} |{" "}
                      {formatDateTimeByRegion(event.commence_time)}
                    </option>
                  ))
                ) : (
                  <option value="">No matchups available</option>
                )}
              </select>
              <button
                className="btn btn-sm btn-outline-dark mb-0"
                onClick={() => {
                  void handleLoadEvents(true);
                }}
                disabled={eventsState.loading}
              >
                {eventsState.loading ? "Refreshing..." : "Refresh Matchups"}
              </button>
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
                Load Matchup Props
              </button>
            </div>
            {eventsState.error && (
              <div className="alert alert-danger text-white" role="alert">
                <strong>Error:</strong> {eventsState.error}
              </div>
            )}
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
                    <span className="text-sm text-secondary">Matchup:</span>
                    <span className="text-sm font-weight-bold ms-2">
                      {propsState.data.data.away_team} @ {propsState.data.data.home_team}
                    </span>
                  </div>
                  <div>
                    <span className="text-sm text-secondary">Bookmakers:</span>
                    <span className="text-sm font-weight-bold ms-2">
                      {propsState.data.data.bookmakers?.length ?? 0}
                    </span>
                  </div>
                </div>
                <div className="table-responsive">
                  {(() => {
                    const rows = normalizeOddsEventProps(propsState.data.data);

                    if (rows.length) {
                      return (
                        <table className="table align-items-center mb-0">
                          <thead>
                            <tr>
                              <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Matchup</th>
                              <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Player</th>
                              <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Market</th>
                              <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Side</th>
                              <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Line</th>
                              <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Price</th>
                              <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Book</th>
                              <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Updated</th>
                            </tr>
                          </thead>
                          <tbody>
                            {rows.map((row, index) => (
                              <tr key={`${row.eventId}-${row.player}-${row.marketKey}-${row.outcomeType}-${row.line}-${index}`}>
                                <td className="text-sm">{row.matchup}</td>
                                <td className="text-sm">
                                  <a
                                    href={getPlayerStatsSearchUrl(row.player)}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className="player-name-link"
                                  >
                                    {row.player}
                                  </a>
                                </td>
                                <td className="text-sm">{row.marketKey}</td>
                                <td className="text-sm">{row.outcomeType}</td>
                                <td className="text-sm">
                                  {row.line !== null ? row.line.toFixed(1) : "-"}
                                </td>
                                <td className="text-sm">
                                  {row.odds !== null ? row.odds.toFixed(2) : "-"}
                                </td>
                                <td className="text-sm">{row.sportsbook}</td>
                                <td className="text-sm">
                                  {row.lastUpdate
                                    ? formatTimeByRegion(row.lastUpdate)
                                    : "-"}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      );
                    }

                    return <p className="text-secondary text-center py-4">No prop outcomes available.</p>;
                  })()}
                </div>
              </>
            )}
          </div>
        );
      case "best_bets":
        const estimatedMarketsPerEvent = includeComboMarkets ? 7 : 3;
        const selectedMatchupCount =
          selectedBestBetEventIds.length > 0
            ? selectedBestBetEventIds.length
            : bestBetEvents;
        const estimatedCredits = selectedMatchupCount * estimatedMarketsPerEvent;
        const bestBetPhaseLabel =
          bestBetsProgressData?.message ??
          (bestBetLoadingPhase === "syncing"
            ? `Syncing Sportsbet props${bestBetFocusLabel ? ` for ${bestBetFocusLabel.label}` : ""}`
            : bestBetLoadingPhase === "ranking"
              ? `Ranking parlay combinations${bestBetFocusLabel ? ` around ${bestBetFocusLabel.label}` : ""}`
              : `Scoring candidate legs${bestBetFocusLabel ? ` for ${bestBetFocusLabel.label}` : ""}`);
        const bestBetLoadingSummary =
          bestBetsProgressData?.phase
            ? `${bestBetsProgressData.phase[0].toUpperCase()}${bestBetsProgressData.phase.slice(1)}`
            : bestBetLoadingPhase === "syncing"
              ? "Syncing"
              : bestBetLoadingPhase === "ranking"
                ? "Ranking"
                : "Scoring";
        return (
          <div className="card card-body border-radius-xl shadow-lg best-bets-panel">
            <div className="d-flex flex-column flex-lg-row justify-content-between align-items-start gap-3 mb-4">
              <div>
                <h4 className="mb-1">Best Bet Builder</h4>
              </div>
              <div className="best-bets-controls">
                <div className="control-group">
                  <label className="form-label mb-1">Target payout (x)</label>
                  <input
                    type="number"
                    min="1.1"
                    step="0.1"
                    className="form-control form-control-sm"
                    value={targetMultiplierInput}
                    onChange={(e) => setTargetMultiplierInput(e.target.value)}
                  />
                </div>
                <div className="control-group">
                  <label className="form-label mb-1">Legs</label>
                  <select
                    className="form-select form-select-sm"
                    value={bestBetLegCount}
                    onChange={(e) => setBestBetLegCount(Number(e.target.value))}
                  >
                    {[1, 2, 3, 4].map((count) => (
                      <option key={count} value={count}>
                        {count}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="control-group">
                  <label className="form-label mb-1">Leg mode</label>
                  <button
                    type="button"
                    className={`combo-toggle ${wildcardLegs ? "active" : ""}`}
                    onClick={() => setWildcardLegs((prev) => !prev)}
                    aria-pressed={wildcardLegs}
                  >
                    <span className="combo-toggle-title">
                      {wildcardLegs ? `Any up to ${bestBetLegCount}` : "Exact legs"}
                    </span>
                    <span className="combo-toggle-state">
                      {wildcardLegs ? "Any" : "Exact"}
                    </span>
                  </button>
                </div>
                <div className="control-group">
                  <label className="form-label mb-1">Time of Day</label>
                  <select
                    className="form-select form-select-sm"
                    value={bestBetSyncMode}
                    onChange={(e) =>
                      setBestBetSyncMode(
                        e.target.value as "auto" | "night" | "morning" | "all"
                      )
                    }
                    disabled={selectedBestBetEventIds.length > 0}
                  >
                    <option value="auto">Auto ({regionConfig.short} time)</option>
                    <option value="night">Night (next slate)</option>
                    <option value="morning">Morning (near tipoff)</option>
                    <option value="all">All upcoming</option>
                  </select>
                </div>
                <div className="control-group">
                  <label className="form-label mb-1">Market scope</label>
                  <button
                    type="button"
                    className={`combo-toggle ${includeComboMarkets ? "active" : ""}`}
                    onClick={() => setIncludeComboMarkets((prev) => !prev)}
                    aria-pressed={includeComboMarkets}
                  >
                    <span className="combo-toggle-title">Combo markets</span>
                    <span className="combo-toggle-state">
                      {includeComboMarkets ? "On" : "Off"}
                    </span>
                  </button>
                </div>
              </div>
            </div>
            <div className="best-bets-matchups mb-3">
              <div className="d-flex align-items-center justify-content-between mb-2">
                <label className="form-label mb-0">Matchups</label>
                <div className="d-flex gap-2">
                  <button
                    className="btn btn-sm btn-outline-dark mb-0"
                    onClick={() => {
                      void handleLoadEvents(true);
                    }}
                    disabled={eventsState.loading}
                  >
                    {eventsState.loading ? "Refreshing..." : "Refresh"}
                  </button>
                  <button
                    className="btn btn-sm btn-outline-dark mb-0"
                    onClick={() =>
                      setSelectedBestBetEventIds(
                        (eventsState.data ?? []).slice(0, bestBetEvents).map((e) => e.id)
                      )
                    }
                    disabled={!eventsState.data || eventsState.data.length === 0}
                  >
                    Use top {bestBetEvents}
                  </button>
                  <button
                    className="btn btn-sm btn-outline-dark mb-0"
                    onClick={() => setSelectedBestBetEventIds([])}
                    disabled={selectedBestBetEventIds.length === 0}
                  >
                    Clear
                  </button>
                </div>
              </div>
              {eventsState.error && (
                <p className="text-xs text-danger mb-2">Matchup load failed: {eventsState.error}</p>
              )}
              <p className="text-xs text-secondary mb-2">
                {selectedBestBetEventIds.length > 0
                  ? `${selectedBestBetEventIds.length} selected`
                  : "No explicit selection (uses Day + Max events)."}
              </p>
              <div className="matchup-chip-wrap">
                {bestBetMatchupOptions.map((event) => {
                  const active = selectedBestBetEventIds.includes(event.id);
                  return (
                    <button
                      key={event.id}
                      type="button"
                      className={`matchup-chip ${active ? "active" : ""}`}
                      onClick={() =>
                        setSelectedBestBetEventIds((prev) =>
                          prev.includes(event.id)
                            ? prev.filter((id) => id !== event.id)
                            : [...prev, event.id]
                        )
                      }
                    >
                      <span>{event.label}</span>
                      <small>{event.kickoff}</small>
                    </button>
                  );
                })}
                {bestBetMatchupOptions.length === 0 && (
                  <span className="text-sm text-secondary">No upcoming matchups found.</span>
                )}
              </div>
            </div>
            <div className="d-flex flex-wrap gap-2 mb-3">
              <button
                className="btn btn-sm bg-gradient-primary mb-0"
                onClick={handleSyncAndLoadBestBets}
                disabled={bestBetsState.loading || isSyncingBestBets}
              >
                {isSyncingBestBets
                  ? "Building bets..."
                  : bestBetsState.loading
                    ? "Working..."
                    : "Build Bets"}
              </button>
              <button
                className="btn btn-sm btn-outline-dark mb-0"
                onClick={() => handleLoadBestBets(true)}
                disabled={bestBetsState.loading || isSyncingBestBets}
              >
                Recompute from Stored Props
              </button>
            </div>
            <p className="text-sm text-secondary mb-3">
              Estimated sync cost: ~{estimatedCredits} credits ({selectedMatchupCount} events x{" "}
              {estimatedMarketsPerEvent} markets/event, Sportsbet only)
              {oddsCreditsRemaining !== null ? ` | Credits left: ${oddsCreditsRemaining}` : ""}
            </p>
            {oddsUsage.error && (
              <p className="text-sm text-secondary mb-3">
                Could not load current credits snapshot.
              </p>
            )}
            {(isSyncingBestBets || bestBetsState.loading) && (
              <div className="mb-3">
                <p className="text-sm text-secondary mb-1">
                  {bestBetLoadingSummary}: {bestBetPhaseLabel}
                </p>
                <p className="text-xs text-secondary mb-0">
                  {bestBetsProgressMatchup
                    ? `Current matchup: ${bestBetsProgressMatchup.label}${
                        bestBetsProgressMatchup.kickoff
                          ? ` (${bestBetsProgressMatchup.kickoff})`
                          : ""
                      }`
                    : !bestBetsProgressPhase || bestBetsProgressPhase === "syncing"
                      ? bestBetFocusLabel
                      ? `Focus matchup: ${bestBetFocusLabel.label} (${bestBetFocusLabel.kickoff})`
                      : "Preparing selected matchups."
                      : "Preparing candidate pool."}
                  {bestBetsProgressSummaryParts.length > 0
                    ? ` | ${bestBetsProgressSummaryParts.join(" | ")}`
                    : ""}
                  {` | ${bestBetElapsedSeconds}s elapsed`}
                </p>
              </div>
            )}
            {syncSummary && <p className="text-sm text-secondary mb-3">{syncSummary}</p>}
            {bestBetsState.error && (
              <div className="alert alert-danger text-white" role="alert">
                <strong>Error:</strong> {bestBetsState.error}
              </div>
            )}
            {bestBetsState.loading && !bestBetsState.data && (
              <div className="text-center py-5">
                <div className="spinner-border text-primary" role="status">
                  <span className="visually-hidden">Loading...</span>
                </div>
                <p className="text-secondary mt-3">Building best bets...</p>
              </div>
            )}
            {bestBetsState.data && (
              <div className="best-bets-results">
                {bestBetsState.data.status !== "ok" ? (
                  <div className="alert alert-warning text-dark mb-3" role="alert">
                    {bestBetsState.data.message ?? "No bets available for current filters."}
                  </div>
                ) : (
                  <>
                    <div className="d-flex flex-wrap gap-3 mb-3">
                      <span className="badge badge-sm bg-gradient-info">
                        Pool: {bestBetsState.data.pool_size ?? 0}
                      </span>
                      <span className="badge badge-sm bg-gradient-secondary">
                        Target: {bestBetsState.data.target_multiplier?.toFixed(2)}x
                      </span>
                      <span className="badge badge-sm bg-gradient-secondary">
                        Legs: {bestBetsState.data.leg_count}
                      </span>
                    </div>
                    <h6 className="mb-2">Recommended Parlays</h6>
                    {(bestBetsState.data.recommended_parlays ?? []).length === 0 ? (
                      <p className="text-sm text-secondary mb-4">
                        No parlay hit the target. Try fewer legs or lower target.
                      </p>
                    ) : (
                      <div className="best-parlays-grid mb-4">
                        {(bestBetsState.data.recommended_parlays ?? []).map((parlay, idx) => (
                          <div key={idx} className="best-parlay-card">
                            <div className="d-flex justify-content-between mb-2">
                              <strong>
                                Parlay #{idx + 1}
                                {parlay.leg_count ? ` (${parlay.leg_count} legs)` : ""}
                              </strong>
                              <span>{parlay.combined_odds.toFixed(2)}x</span>
                            </div>
                            <p className="text-xs text-secondary mb-2">
                              Hit chance {(parlay.combined_probability * 100).toFixed(1)}% | EV{" "}
                              {parlay.expected_value_per_unit.toFixed(2)}
                            </p>
                            <ul className="best-leg-list">
                              {parlay.legs.map((leg) => (
                                <li
                                  key={`${leg.event_id}-${leg.player_name}-${leg.market}-${leg.side}-${leg.line}`}
                                >
                                  <a
                                    href={getPlayerStatsSearchUrl(leg.player_name)}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className="player-name-link"
                                  >
                                    {leg.player_name}
                                  </a>
                                  {leg.prediction.team_abbreviation
                                    ? ` (${leg.prediction.team_abbreviation})`
                                    : ""}{" "}
                                  {leg.side} {leg.line} ({leg.stat_type}) @{" "}
                                  {leg.price_decimal.toFixed(2)}
                                </li>
                              ))}
                            </ul>
                          </div>
                        ))}
                      </div>
                    )}

                    <h6 className="mb-2">Top Single Legs</h6>
                    <div className="table-responsive">
                      <table className="table align-items-center mb-0">
                        <thead>
                          <tr>
                            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Player</th>
                            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Bet</th>
                            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Odds</th>
                            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Model Prob</th>
                            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Edge</th>
                            <th className="text-uppercase text-secondary text-xxs font-weight-bolder opacity-7">Confidence</th>
                          </tr>
                        </thead>
                        <tbody>
                          {(bestBetsState.data.top_single_legs ?? []).map((leg) => (
                            <tr
                              key={`${leg.event_id}-${leg.player_name}-${leg.market}-${leg.side}-${leg.line}`}
                            >
                              <td className="text-sm">
                                <a
                                  href={getPlayerStatsSearchUrl(leg.player_name)}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                  className="player-name-link"
                                >
                                  {leg.player_name}
                                </a>
                                {leg.prediction.team_abbreviation
                                  ? ` (${leg.prediction.team_abbreviation})`
                                  : ""}
                              </td>
                              <td className="text-sm">
                                {leg.side} {leg.line} ({leg.stat_type})
                              </td>
                              <td className="text-sm">{leg.price_decimal.toFixed(2)}</td>
                              <td className="text-sm">{(leg.model_prob * 100).toFixed(1)}%</td>
                              <td className="text-sm">{(leg.edge * 100).toFixed(1)}%</td>
                              <td className="text-sm">
                                {typeof leg.prediction.confidence === "number"
                                  ? `${leg.prediction.confidence}%`
                                  : "n/a"}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </>
                )}
              </div>
            )}
          </div>
        );
      case "predictions":
        const activePrediction = predictionConfig[predictionStat];
        const lineOptions: Record<typeof predictionStat, number[]> = {
          points: [5, 10, 15, 20, 25, 30],
          assists: [2, 3, 4, 6, 7, 10],
          rebounds: [2, 3, 4, 6, 7, 10],
          threept: [1, 2, 3, 4, 5],
          threepa: [2, 4, 6, 8, 10],
        };
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
                predictionLine !== "all" &&
                (row.pred_value ?? 0) < predictionLine
              ) {
                return false;
              }
              if (
                predictionTeams.length > 0 &&
                !predictionTeams.includes(row.team_abbreviation ?? "")
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
                <h4 className="mb-1">Predictions</h4>
                {/* <p className="text-xs text-secondary mb-0">
                  Day filter follows NBA slate timing (ET). Displayed dates/times follow your selected region.
                </p> */}
              </div>
              <div className="d-flex flex-wrap gap-2 align-items-center">
                <div className="stat-toggle">
                  {(["points", "assists", "rebounds", "threept", "threepa"] as const).map((stat) => (
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
                <div className="prediction-select-group">
                  <label className="prediction-select-field">
                    <span className="prediction-select-label">Day</span>
                    <select
                      className="form-select form-select-sm"
                      value={predictionDay}
                      aria-label="Prediction day"
                      onChange={(e) =>
                        setPredictionDay(
                          e.target.value as "today" | "tomorrow" | "yesterday" | "auto"
                        )
                      }
                    >
                      {dayOptions.map((opt) => (
                        <option key={opt.value} value={opt.value}>
                          {opt.label}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="prediction-select-field">
                    <span className="prediction-select-label">Line</span>
                    <select
                      className="form-select form-select-sm"
                      value={predictionLine}
                      aria-label="Prediction line filter"
                      onChange={(e) => {
                        const value =
                          e.target.value === "all"
                            ? "all"
                            : Number(e.target.value);
                        setPredictionLine(value);
                      }}
                    >
                      <option value="all">All lines</option>
                      {lineOptions[predictionStat].map((line) => (
                        <option key={line} value={line}>
                          {line}+
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="prediction-select-field">
                    <span className="prediction-select-label">Sort</span>
                    <select
                      className="form-select form-select-sm"
                      value={predictionSort}
                      aria-label="Prediction sort order"
                      onChange={(e) =>
                        setPredictionSort(
                          e.target.value as
                            | "pred_value_desc"
                            | "pred_value_asc"
                            | "confidence_desc"
                        )
                      }
                    >
                      <option value="pred_value_desc">Value (High &gt;&gt; Low)</option>
                      <option value="pred_value_asc">Value (Low &gt;&gt; High)</option>
                      <option value="confidence_desc">Confidence (High &gt;&gt; Low)</option>
                    </select>
                  </label>
                  <label className="prediction-select-field">
                    <span className="prediction-select-label">Under Risk</span>
                    <select
                      className="form-select form-select-sm"
                      value={underRiskSort}
                      aria-label="Under risk sort order"
                      onChange={(e) =>
                        setUnderRiskSort(
                          e.target.value as "none" | "under_risk_desc" | "under_risk_asc"
                        )
                      }
                    >
                      <option value="none">None</option>
                      <option value="under_risk_desc">High &gt;&gt; Low</option>
                      <option value="under_risk_asc">Low &gt;&gt; High</option>
                    </select>
                  </label>
                </div>
                <button
                  className="btn btn-sm bg-gradient-primary mb-0"
                  onClick={() => {
                    void handleLoadPredictions();
                  }}
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
                    predictionTeams.length === 0 ? "active" : ""
                  }`}
                  onClick={() => setPredictionTeams([])}
                >
                  All teams
                </button>
                {teamOptions.map((team) => (
                  <button
                    key={team}
                    className={`team-chip ${
                      predictionTeams.includes(team) ? "active" : ""
                    }`}
                    onClick={() =>
                      setPredictionTeams((prev) =>
                        prev.includes(team)
                          ? prev.filter((t) => t !== team)
                          : [...prev, team]
                      )
                    }
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
                    const valA = a.pred_value ?? 0;
                    const valB = b.pred_value ?? 0;
                    const confA = a.confidence ?? -1;
                    const confB = b.confidence ?? -1;
                    const riskA = a.under_risk ?? -1;
                    const riskB = b.under_risk ?? -1;

                    if (predictionSort === "pred_value_asc" && valA !== valB) {
                      return valA - valB;
                    }
                    if (predictionSort === "pred_value_desc" && valA !== valB) {
                      return valB - valA;
                    }
                    if (predictionSort === "confidence_desc" && confA !== confB) {
                      return confB - confA;
                    }

                    if (underRiskSort === "under_risk_desc" && riskA !== riskB) {
                      return riskB - riskA;
                    }
                    if (underRiskSort === "under_risk_asc" && riskA !== riskB) {
                      return riskA - riskB;
                    }

                    return valB - valA;
                  })}
                  statLabel={activePrediction.label}
                  unitLabel={activePrediction.unit}
                  statKey={predictionStat}
                  formatGameDate={formatSlateDateForRegion}
                />
            )}
          </div>
        );
      case "matchups":
        const matchupPrediction = predictionConfig[predictionStat];
        const matchupRows = matchupPrediction.state.data
          ? matchupPrediction.state.data
              .filter(
                (row) => normalizeMatchupKey(row.matchup) === selectedPredictionMatchup
              )
              .sort((a, b) => (b.pred_value ?? 0) - (a.pred_value ?? 0))
          : [];
        return (
          <div className="card card-body border-radius-xl shadow-lg prediction-focus">
            <div className="d-flex flex-column flex-lg-row justify-content-between align-items-start gap-3 mb-4">
              <div>
                <h4 className="mb-1">Matchup Predictions</h4>
                <p className="text-sm text-secondary mb-0">
                  View both teams together for one matchup on the selected slate.
                </p>
              </div>
              <div className="d-flex flex-wrap gap-2 align-items-center">
                <div className="stat-toggle">
                  {(["points", "assists", "rebounds", "threept", "threepa"] as const).map((stat) => (
                    <button
                      key={stat}
                      className={`stat-chip ${predictionStat === stat ? "active" : ""}`}
                      onClick={() => setPredictionStat(stat)}
                    >
                      {predictionConfig[stat].label}
                    </button>
                  ))}
                </div>
                <div className="prediction-select-group">
                  <label className="prediction-select-field">
                    <span className="prediction-select-label">Day</span>
                    <select
                      className="form-select form-select-sm"
                      value={predictionDay}
                      aria-label="Matchup prediction day"
                      onChange={(e) =>
                        setPredictionDay(
                          e.target.value as "today" | "tomorrow" | "yesterday" | "auto"
                        )
                      }
                    >
                      {dayOptions.map((opt) => (
                        <option key={opt.value} value={opt.value}>
                          {opt.label}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="prediction-select-field">
                    <span className="prediction-select-label">Sort</span>
                    <select
                      className="form-select form-select-sm"
                      value={predictionSort}
                      aria-label="Matchup prediction sort order"
                      onChange={(e) =>
                        setPredictionSort(
                          e.target.value as
                            | "pred_value_desc"
                            | "pred_value_asc"
                            | "confidence_desc"
                        )
                      }
                    >
                      <option value="pred_value_desc">Value (High &gt;&gt; Low)</option>
                      <option value="pred_value_asc">Value (Low &gt;&gt; High)</option>
                      <option value="confidence_desc">Confidence (High &gt;&gt; Low)</option>
                    </select>
                  </label>
                </div>
                <button
                  className="btn btn-sm bg-gradient-primary mb-0"
                  onClick={() => {
                    void handleLoadPredictions(true);
                  }}
                  disabled={matchupPrediction.state.loading}
                >
                  {matchupPrediction.state.loading ? (
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
                      sports_basketball
                    </i>
                  )}
                  Refresh Matchups
                </button>
              </div>
            </div>
            <div className="best-bets-matchups mb-4">
              <label className="form-label">Select matchup</label>
              <p className="text-xs text-secondary mb-2">
                {bestBetMatchupOptions.length > 0
                  ? `Event times loaded: ${bestBetMatchupOptions.length}`
                  : "No event times available (showing slate dates)."}
              </p>
              <div className="matchup-chip-wrap">
                {currentPredictionMatchups.map((item) => (
                  <button
                    key={item.key}
                    type="button"
                    className={`matchup-chip ${selectedPredictionMatchup === item.key ? "active" : ""}`}
                    onClick={() => setSelectedPredictionMatchup(item.key)}
                  >
                    <span>{item.label}</span>
                    <small>{item.kickoff}</small>
                  </button>
                ))}
                {currentPredictionMatchups.length === 0 && (
                  <span className="text-sm text-secondary">No matchups available for this slate.</span>
                )}
              </div>
            </div>
            {selectedPredictionMatchup && (
              <div className="mb-3">
                <span className="badge badge-sm bg-gradient-info me-2">
                  {selectedPredictionMatchupOption?.label ?? selectedPredictionMatchup}
                </span>
                <span className="text-sm text-secondary">
                  {matchupRows.length} player predictions
                </span>
              </div>
            )}
            {matchupPrediction.state.error && (
              <div className="alert alert-danger text-white" role="alert">
                <strong>Error:</strong> {matchupPrediction.state.error}
              </div>
            )}
            {matchupPrediction.state.loading && !matchupPrediction.state.data && (
              <div className="text-center py-5">
                <div className="spinner-border text-primary" role="status">
                  <span className="visually-hidden">Loading...</span>
                </div>
                <p className="text-secondary mt-3">Loading matchup predictions...</p>
              </div>
            )}
            {!matchupPrediction.state.loading &&
              matchupPrediction.state.data &&
              selectedPredictionMatchup &&
              matchupRows.length === 0 && (
                <div className="text-center py-5">
                  <p className="text-secondary mb-0">No predictions found for that matchup.</p>
                </div>
              )}
            {matchupRows.length > 0 && (
              <PredictionsGrid
                predictions={[...matchupRows].sort((a, b) => {
                  const valA = a.pred_value ?? 0;
                  const valB = b.pred_value ?? 0;
                  const confA = a.confidence ?? -1;
                  const confB = b.confidence ?? -1;

                  if (predictionSort === "pred_value_asc" && valA !== valB) {
                    return valA - valB;
                  }
                  if (predictionSort === "pred_value_desc" && valA !== valB) {
                    return valB - valA;
                  }
                  if (predictionSort === "confidence_desc" && confA !== confB) {
                    return confB - confA;
                  }
                  return valB - valA;
                })}
                statLabel={matchupPrediction.label}
                unitLabel={matchupPrediction.unit}
                statKey={predictionStat}
                formatGameDate={formatSlateDateForRegion}
              />
            )}
          </div>
        );
      case "double_triple":
        const dtSearch = predictionSearch.trim().toLowerCase();
        const dtRows = doubleTripleState.data
          ? doubleTripleState.data.filter((row) => {
              if (
                predictionTeams.length > 0 &&
                !predictionTeams.includes(row.team_abbreviation ?? "")
              ) {
                return false;
              }
              if (!dtSearch) return true;
              return (
                row.full_name.toLowerCase().includes(dtSearch) ||
                row.team_abbreviation.toLowerCase().includes(dtSearch)
              );
            })
          : [];
        const dtTeamOptions = doubleTripleState.data
          ? Array.from(
              new Set(
                doubleTripleState.data
                  .map((row) => row.team_abbreviation)
                  .filter((team): team is string => Boolean(team))
              )
            ).sort()
          : [];
        return (
          <div className="card card-body border-radius-xl shadow-lg prediction-focus">
            <div className="d-flex flex-column flex-lg-row justify-content-between align-items-start gap-3 mb-4">
              <div>
                <h4 className="mb-1">Double-Double and Triple-Double Outlook</h4>
              </div>
              <div className="d-flex flex-wrap gap-2 align-items-center">
                <div className="prediction-select-group">
                  <select
                    className="form-select form-select-sm"
                    value={predictionDay}
                    onChange={(e) =>
                      setPredictionDay(
                        e.target.value as "today" | "tomorrow" | "yesterday" | "auto"
                      )
                    }
                  >
                    {dayOptions.map((opt) => (
                      <option key={opt.value} value={opt.value}>
                        {opt.label}
                      </option>
                    ))}
                  </select>
                </div>
                <button
                  className="btn btn-sm bg-gradient-primary mb-0"
                  onClick={() => {
                    void handleLoadDoubleTriple();
                  }}
                  disabled={doubleTripleState.loading}
                >
                  Get DD/TD
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
                  className={`team-chip ${predictionTeams.length === 0 ? "active" : ""}`}
                  onClick={() => setPredictionTeams([])}
                >
                  All teams
                </button>
                {dtTeamOptions.map((team) => (
                  <button
                    key={team}
                    className={`team-chip ${predictionTeams.includes(team) ? "active" : ""}`}
                    onClick={() =>
                      setPredictionTeams((prev) =>
                        prev.includes(team)
                          ? prev.filter((t) => t !== team)
                          : [...prev, team]
                      )
                    }
                  >
                    {team}
                  </button>
                ))}
              </div>
            </div>
            {doubleTripleState.error && (
              <div className="alert alert-danger text-white" role="alert">
                <strong>Error:</strong> {doubleTripleState.error}
              </div>
            )}
            {doubleTripleState.loading && !doubleTripleState.data && (
              <div className="text-center py-5">
                <div className="spinner-border text-primary" role="status">
                  <span className="visually-hidden">Loading...</span>
                </div>
                <p className="text-secondary mt-3">Loading DD/TD outlook...</p>
              </div>
            )}
            {doubleTripleState.data && (
              <DoubleTripleGrid
                rows={[...dtRows].sort((a, b) => {
                  const dd = (b.double_double_prob ?? 0) - (a.double_double_prob ?? 0);
                  if (dd !== 0) return dd;
                  return (b.triple_double_prob ?? 0) - (a.triple_double_prob ?? 0);
                })}
              />
            )}
          </div>
        );
      case "first_basket":
        const fbTeamOptions = firstBasketPredictionsState.data
          ? Array.from(
              new Set(
                firstBasketPredictionsState.data
                  .map((row) => row.team_abbreviation)
                  .filter((team): team is string => Boolean(team))
              )
            ).sort()
          : [];
        const fbSearch = predictionSearch.trim().toLowerCase();
        const filteredFirstBasket = firstBasketPredictionsState.data
          ? firstBasketPredictionsState.data.filter((row) => {
              if (
                predictionTeams.length > 0 &&
                !predictionTeams.includes(row.team_abbreviation ?? "")
              ) {
                return false;
              }
              if (!fbSearch) return true;
              return (
                row.full_name.toLowerCase().includes(fbSearch) ||
                row.team_abbreviation.toLowerCase().includes(fbSearch)
              );
            })
          : [];
        const bestFirstBasketPlays = [...filteredFirstBasket]
          .sort((a, b) => {
            const scoreA =
              (a.first_basket_prob ?? 0) * (0.7 + 0.3 * (a.team_scores_first_prob ?? 0));
            const scoreB =
              (b.first_basket_prob ?? 0) * (0.7 + 0.3 * (b.team_scores_first_prob ?? 0));
            return scoreB - scoreA;
          })
          .slice(0, 5);
        return (
          <div className="card card-body border-radius-xl shadow-lg prediction-focus">
            <div className="d-flex flex-column flex-lg-row justify-content-between align-items-start gap-3 mb-4">
              <div>
                <h4 className="mb-1">First Basket Predictions</h4>
              </div>
              <div className="d-flex flex-wrap gap-2 align-items-center">
                <div className="prediction-select-group">
                  <select
                    className="form-select form-select-sm"
                    value={predictionDay}
                    onChange={(e) =>
                      setPredictionDay(
                        e.target.value as "today" | "tomorrow" | "yesterday" | "auto"
                      )
                    }
                  >
                    {dayOptions.map((opt) => (
                      <option key={opt.value} value={opt.value}>
                        {opt.label}
                      </option>
                    ))}
                  </select>
                  <select
                    className="form-select form-select-sm"
                    value={firstBasketSort}
                    onChange={(e) =>
                      setFirstBasketSort(e.target.value as "prob_desc" | "prob_asc")
                    }
                  >
                    <option value="prob_desc">First Basket % (High &gt;&gt; Low)</option>
                    <option value="prob_asc">First Basket % (Low &gt;&gt; High)</option>
                  </select>
                </div>
                <button
                  className="btn btn-sm bg-gradient-primary mb-0"
                  onClick={() => {
                    void handleLoadFirstBasketPredictions();
                  }}
                  disabled={firstBasketPredictionsState.loading}
                >
                  Get First Basket
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
                  className={`team-chip ${predictionTeams.length === 0 ? "active" : ""}`}
                  onClick={() => setPredictionTeams([])}
                >
                  All teams
                </button>
                {fbTeamOptions.map((team) => (
                  <button
                    key={team}
                    className={`team-chip ${predictionTeams.includes(team) ? "active" : ""}`}
                    onClick={() =>
                      setPredictionTeams((prev) =>
                        prev.includes(team)
                          ? prev.filter((t) => t !== team)
                          : [...prev, team]
                      )
                    }
                  >
                    {team}
                  </button>
                ))}
              </div>
            </div>
            {firstBasketPredictionsState.error && (
              <div className="alert alert-danger text-white" role="alert">
                <strong>Error:</strong> {firstBasketPredictionsState.error}
              </div>
            )}
            {firstBasketPredictionsState.loading && !firstBasketPredictionsState.data && (
              <div className="text-center py-5">
                <div className="spinner-border text-primary" role="status">
                  <span className="visually-hidden">Loading...</span>
                </div>
              </div>
            )}
            {firstBasketPredictionsState.data && (
              <>
                <div className="mb-4">
                  <h6 className="mb-2">Best First Basket Plays</h6>
                  <div className="d-flex flex-wrap gap-2">
                    {bestFirstBasketPlays.map((row) => (
                      <span
                        key={`best-${row.game_id ?? row.matchup}-${row.player_id}`}
                        className="badge bg-gradient-success"
                      >
                        <a
                          href={getPlayerStatsSearchUrl(row.full_name)}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="player-name-link"
                        >
                          {row.full_name}
                        </a>{" "}
                        ({row.team_abbreviation}) -{" "}
                        {(row.first_basket_prob * 100).toFixed(1)}%
                      </span>
                    ))}
                  </div>
                </div>
                <FirstBasketGrid
                  rows={[...filteredFirstBasket].sort((a, b) => {
                    const pa = a.first_basket_prob ?? 0;
                    const pb = b.first_basket_prob ?? 0;
                    return firstBasketSort === "prob_asc" ? pa - pb : pb - pa;
                  })}
                  userRegion={userRegion}
                />
              </>
            )}
          </div>
        );
      default:
        return null;
    }
  };

  return (
    <div className="app-shell min-vh-100">
      <header className="hero-header position-relative overflow-hidden">
        <div className="hero-glow"></div>
        <div className="container position-relative">
          <nav className="navbar navbar-expand-lg navbar-dark py-4 px-0">
            <div className="brand-hero brand-hero-left">
              <img src={logo} alt="Gamblr logo" className="brand-logo brand-logo-xl" />
              <div className="brand-text-wrap">
                <h2 className="mb-0 text-white brand-title">GAMBLR</h2>
              </div>
            </div>
            <div className="ms-auto d-flex flex-column align-items-end gap-2">
              <div className="d-flex align-items-center gap-3">
                <a className="nav-link text-white opacity-9 px-0 py-1" href="/">
                  Home
                </a>
                <a className="nav-link text-white opacity-9 px-0 py-1" href="/performance">
                  How We&apos;re Doing
                </a>
                <a className="nav-link text-white opacity-9 px-0 py-1" href="#about">
                  About
                </a>
              </div>
              <div className="d-flex align-items-center gap-2">
                <label className="text-xs text-white opacity-8 mb-0" htmlFor="region-select">
                  Region
                </label>
                <select
                  id="region-select"
                  className="form-select form-select-sm region-select"
                  value={userRegion}
                  onChange={(e) => setUserRegion(e.target.value as UserRegion)}
                >
                  <option value="au">Australia</option>
                  <option value="us">USA</option>
                  <option value="uk">England</option>
                </select>
              </div>
            </div>
          </nav>

          <div className="row">
            <div className="col-lg-10 col-xl-8">
              <div className="hero-copy">
                <h1 className="display-4 text-white mb-3">
                  Data Over Luck.
                </h1>
                <p className="lead text-white opacity-8 mb-4">
                  I built this to make NBA props easier to read: points, assists, rebounds,
                  and threes in one place so you can make faster calls.
                </p>
                <p className="text-sm text-white opacity-8 mb-0">
                  This app runs on free-tier hosting, so the first load can take
                  around 30-60 seconds while services wake up.
                </p>
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
               
                    <h3 className="mb-1">Performance Hub</h3>
                    
                  <div className="d-flex flex-wrap gap-2"></div>
                </div>
                <div className="nav-wrapper position-relative mt-4">
                  <ul className="nav nav-pills flex-wrap p-1 tab-pills" role="tablist">
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
                    <li className="nav-item">
                      <a
                        className={`nav-link mb-0 px-0 py-1 ${activeTab === "matchups" ? "active" : ""}`}
                        onClick={() => setActiveTab("matchups")}
                        role="tab"
                        style={{ cursor: "pointer" }}
                      >
                        <i className="material-symbols-rounded me-2">groups</i>
                        Matchups
                      </a>
                    </li>
                    <li className="nav-item">
                      <a
                        className={`nav-link mb-0 px-0 py-1 ${activeTab === "best_bets" ? "active" : ""}`}
                        onClick={() => setActiveTab("best_bets")}
                        role="tab"
                        style={{ cursor: "pointer" }}
                      >
                        <i className="material-symbols-rounded me-2">paid</i>
                        Best Bets
                      </a>
                    </li>
                    <li className="nav-item">
                      <a
                        className={`nav-link mb-0 px-0 py-1 ${activeTab === "first_basket" ? "active" : ""}`}
                        onClick={() => setActiveTab("first_basket")}
                        role="tab"
                        style={{ cursor: "pointer" }}
                      >
                        <i className="material-symbols-rounded me-2">sports</i>
                        First Basket
                      </a>
                    </li>
                    <li className="nav-item">
                      <a
                        className={`nav-link mb-0 px-0 py-1 ${activeTab === "double_triple" ? "active" : ""}`}
                        onClick={() => setActiveTab("double_triple")}
                        role="tab"
                        style={{ cursor: "pointer" }}
                      >
                        <i className="material-symbols-rounded me-2">query_stats</i>
                        DD/TD
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
                  </ul>
                </div>
              </div>

              <main>{renderContent()}</main>
            </div>
            <div className="col-lg-4">
              <div className="card shadow-lg border-radius-xl mb-4">
                <div className="card-header pb-0">
                  <h5 className="mb-0">Backend Status</h5>
                </div>
                <div className="card-body">
                  <div className="status-row mb-2">
                    <span className="text-sm text-secondary">Status</span>
                    <span
                      className={`badge badge-sm ${
                        backendHealth === "up"
                          ? "bg-gradient-success"
                          : backendHealth === "down"
                            ? "bg-gradient-warning"
                            : "bg-gradient-info"
                      }`}
                    >
                      {backendHealth === "up"
                        ? "Online"
                        : backendHealth === "down"
                          ? "Sleeping / Down"
                          : "Checking..."}
                    </span>
                  </div>
                  {backendCheckedAt && (
                    <p className="text-xs text-secondary mb-2">
                      Last checked: {formatTimeByRegion(backendCheckedAt)}
                    </p>
                  )}
                  {backendError && (
                    <p className="text-xs text-warning mb-3">
                      {backendError}
                    </p>
                  )}
                  <p className="text-xs text-secondary mb-3">
                    Free-tier backend may sleep. First request can take 30-60s while it wakes up.
                  </p>
                  <button
                    className="btn btn-sm btn-outline-dark w-100"
                    onClick={() => {
                      void checkBackendHealth();
                    }}
                  >
                    Check Backend Now
                  </button>
                </div>
              </div>
              <div className="card shadow-lg border-radius-xl mb-4" id="about">
                <div className="card-header pb-0">
                  <h5 className="mb-0">How to Read This</h5>
                </div>
                <div className="card-body">
                  <p className="text-sm text-secondary mb-2">
                    Confidence is just a guide, not a guarantee. I usually trust picks more
                    when confidence, line value, and player role all point the same way.
                  </p>
                  <p className="text-sm text-secondary mb-3">
                    Always recheck after lineup or injury news before lock. Minutes can change
                    quickly and that is usually what breaks a good pick.
                  </p>
                  <div className="d-flex align-items-center justify-content-between">
                    <span className="text-sm text-secondary">Data refresh</span>
                    <span className="badge badge-sm bg-gradient-info">Every Night at 12:00 AM</span>
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
