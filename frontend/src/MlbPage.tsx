import { useEffect, useState } from "react";
import "./App.css";
import {
  getMlbHrEvBoard,
  getMlbMarkets,
  getMlbPredictionSlate,
  type MlbHrEvBoardResponse,
  type MlbHrEvRow,
  type MlbMarketName,
  type MlbMarketStatus,
  type MlbPredictionSlateResponse,
  type MlbPredictionRow,
} from "./api";
import logo from "./assets/logo2.png";

type UserRegion = "au" | "us" | "uk";
type MainTab = "predictions" | "home_run_ev" | "model_status";
type MlbDay = "auto" | "today" | "tomorrow" | "yesterday";
type MlbSort = "value_desc" | "value_asc" | "lineup_asc" | "player_az";
type ApiState<T> = {
  data: T | null;
  loading: boolean;
  error: string | null;
};

const REGION_TIMEZONE: Record<UserRegion, string> = {
  au: "Australia/Sydney",
  us: "America/New_York",
  uk: "Europe/London",
};

const REGION_SHORT: Record<UserRegion, string> = {
  au: "AET",
  us: "ET",
  uk: "UK",
};

const DAY_OPTIONS: Array<{ value: MlbDay; label: string }> = [
  { value: "auto", label: "Auto" },
  { value: "today", label: "Today" },
  { value: "tomorrow", label: "Tomorrow" },
  { value: "yesterday", label: "Yesterday" },
];

const SORT_OPTIONS: Array<{ value: MlbSort; label: string }> = [
  { value: "value_desc", label: "Value (High >> Low)" },
  { value: "value_asc", label: "Value (Low >> High)" },
  { value: "lineup_asc", label: "Lineup Order" },
  { value: "player_az", label: "Player A-Z" },
];

const DAY_MS = 24 * 60 * 60 * 1000;

const MARKET_CONFIG: Record<
  MlbMarketName,
  { label: string; tabLabel: string; shortLabel: string; unit: string; icon: string; valueKey: "probability" | "prediction" }
> = {
  batter_home_runs: {
    label: "Home Runs",
    tabLabel: "Home Runs",
    shortLabel: "HR",
    unit: "chance",
    icon: "sports_baseball",
    valueKey: "probability",
  },
  batter_hits: {
    label: "Hits",
    tabLabel: "Hits",
    shortLabel: "Hits",
    unit: "hits",
    icon: "ads_click",
    valueKey: "prediction",
  },
  batter_total_bases: {
    label: "Total Bases",
    tabLabel: "Total Bases",
    shortLabel: "Bases",
    unit: "bases",
    icon: "analytics",
    valueKey: "prediction",
  },
  pitcher_strikeouts: {
    label: "Pitcher Strikeouts",
    tabLabel: "Strikeouts",
    shortLabel: "Ks",
    unit: "Ks",
    icon: "bolt",
    valueKey: "prediction",
  },
};

const initialPredictionsState: ApiState<MlbPredictionSlateResponse> = {
  data: null,
  loading: false,
  error: null,
};

const initialEvState: ApiState<MlbHrEvBoardResponse> = {
  data: null,
  loading: false,
  error: null,
};

function getDatePartsInTimeZone(date: Date, timeZone: string) {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  const parts = formatter.formatToParts(date);
  return {
    year: Number(parts.find((part) => part.type === "year")?.value),
    month: Number(parts.find((part) => part.type === "month")?.value),
    day: Number(parts.find((part) => part.type === "day")?.value),
  };
}

function toUtcMidnightMs(parts: { year: number; month: number; day: number }) {
  return Date.UTC(parts.year, parts.month - 1, parts.day);
}

function dateValueFromParts(parts: { year: number; month: number; day: number }) {
  const month = String(parts.month).padStart(2, "0");
  const day = String(parts.day).padStart(2, "0");
  return `${parts.year}-${month}-${day}`;
}

function addDaysToDateValue(dateValue: string, days: number) {
  const date = new Date(`${dateValue}T12:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function resolveMlbSlateDate(day: MlbDay, region: UserRegion) {
  const selectedOffset: Record<Exclude<MlbDay, "auto">, number> = {
    yesterday: -1,
    today: 0,
    tomorrow: 1,
  };
  const now = new Date();
  const mlbToday = getDatePartsInTimeZone(now, "America/New_York");
  const userToday = getDatePartsInTimeZone(now, REGION_TIMEZONE[region]);
  const mlbTodayValue = dateValueFromParts(mlbToday);
  const userMlbDeltaDays = Math.round((toUtcMidnightMs(userToday) - toUtcMidnightMs(mlbToday)) / DAY_MS);

  if (region === "au") {
    const auStillAheadOfMlbDate = userMlbDeltaDays >= 1;
    if (day === "auto") {
      return addDaysToDateValue(mlbTodayValue, auStillAheadOfMlbDate ? 0 : -1);
    }
    if (auStillAheadOfMlbDate) {
      return addDaysToDateValue(mlbTodayValue, selectedOffset[day]);
    }
    if (day === "yesterday") return addDaysToDateValue(mlbTodayValue, -2);
    if (day === "today") return addDaysToDateValue(mlbTodayValue, -1);
    return mlbTodayValue;
  }

  if (day === "auto") {
    return mlbTodayValue;
  }
  return addDaysToDateValue(mlbTodayValue, selectedOffset[day]);
}

function formatGameTime(value: string | null | undefined, region: UserRegion) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return new Intl.DateTimeFormat("en-AU", {
    timeZone: REGION_TIMEZONE[region],
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

function formatGameDateTime(value: string | null | undefined, region: UserRegion) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return new Intl.DateTimeFormat("en-AU", {
    timeZone: REGION_TIMEZONE[region],
    weekday: "short",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

function formatPct(value?: number | null, digits = 1) {
  if (typeof value !== "number" || Number.isNaN(value)) return "-";
  return `${(value * 100).toFixed(digits)}%`;
}

function formatMoney(value?: number | null) {
  if (typeof value !== "number" || Number.isNaN(value)) return "-";
  return value >= 0 ? `+$${value.toFixed(2)}` : `-$${Math.abs(value).toFixed(2)}`;
}

function formatAmerican(value?: number | null) {
  if (typeof value !== "number" || Number.isNaN(value)) return "-";
  return value > 0 ? `+${value}` : String(value);
}

function formatMarketValue(row: MlbPredictionRow, market: MlbMarketName) {
  const config = MARKET_CONFIG[market];
  const rawValue = row[config.valueKey];
  if (typeof rawValue !== "number" || Number.isNaN(rawValue)) return "-";
  if (config.valueKey === "probability") return formatPct(rawValue);
  return rawValue.toFixed(1);
}

function getInitials(name: string | null | undefined) {
  return (
    (name ?? "")
      .split(" ")
      .filter(Boolean)
      .slice(0, 2)
      .map((part) => part[0])
      .join("")
      .toUpperCase() || "MLB"
  );
}

function getMatchup(row: MlbPredictionRow) {
  const team = row.team_abbreviation ?? "-";
  const opponent = row.opponent_team_abbreviation ?? "-";
  return row.is_home ? `${opponent} @ ${team}` : `${team} @ ${opponent}`;
}

function sortPredictionRows(rows: MlbPredictionRow[], market: MlbMarketName, sort: MlbSort) {
  const config = MARKET_CONFIG[market];
  return [...rows].sort((a, b) => {
    const valueA = a[config.valueKey] ?? -1;
    const valueB = b[config.valueKey] ?? -1;
    if (sort === "value_asc") return valueA - valueB;
    if (sort === "lineup_asc") {
      const orderA = a.batting_order ?? Number.MAX_SAFE_INTEGER;
      const orderB = b.batting_order ?? Number.MAX_SAFE_INTEGER;
      if (orderA !== orderB) return orderA - orderB;
      return valueB - valueA;
    }
    if (sort === "player_az") {
      return (a.player_name ?? "").localeCompare(b.player_name ?? "");
    }
    return valueB - valueA;
  });
}

function MlbPredictionsGrid({
  rows,
  market,
  userRegion,
}: {
  rows: MlbPredictionRow[];
  market: MlbMarketName;
  userRegion: UserRegion;
}) {
  const config = MARKET_CONFIG[market];
  if (!rows.length) {
    return (
      <div className="text-center py-5">
        <p className="text-secondary">No MLB predictions available for this date.</p>
      </div>
    );
  }

  return (
    <div className="predictions-grid mt-4">
      {rows.map((row) => (
        <div key={`${market}-${row.game_pk}-${row.player_id}`} className="card card-body border-radius-xl shadow-lg">
          <div className="d-flex justify-content-between align-items-start mb-3">
            <div className="flex-grow-1">
              <div className="d-flex align-items-center gap-2 mb-1">
                <div className="player-avatar mlb-player-avatar">
                  <span className="avatar-fallback">{getInitials(row.player_name)}</span>
                </div>
                <div>
                  <h5 className="mb-1">{row.player_name}</h5>
                  <span className="badge badge-sm bg-gradient-primary mb-2">
                    {row.team_abbreviation ?? "-"}
                  </span>
                </div>
              </div>
            </div>
            <div className="text-end">
              <h2 className="mb-0 text-gradient text-primary">{formatMarketValue(row, market)}</h2>
              <span className="text-xs text-secondary">{config.unit}</span>
            </div>
          </div>

          <div className="border-top pt-3">
            <div className="d-flex align-items-center mb-2">
              <i className="material-symbols-rounded text-primary me-2">sports_baseball</i>
              <span className="text-sm font-weight-bold">{getMatchup(row)}</span>
            </div>
            <div className="d-flex align-items-center mb-2">
              <i className="material-symbols-rounded text-secondary me-2">calendar_today</i>
              <span className="text-sm text-secondary">
                {formatGameDateTime(row.start_time_utc, userRegion)} ({REGION_SHORT[userRegion]})
              </span>
            </div>
            <p className="text-xs text-secondary mb-2">MLB/US slate date: {row.game_date}</p>
            <div className="prediction-stat-tag mt-3">
              <span className="badge badge-sm bg-gradient-info">{config.label}</span>
            </div>

            <div className="prediction-band confidence-mid mt-3">
              <div className="prediction-band-row">
                <span className="label">{market === "pitcher_strikeouts" ? "Probable starter" : "Lineup source"}</span>
                <span className="value">
                  {market === "pitcher_strikeouts"
                    ? row.starter_pitcher_id
                      ? "Confirmed"
                      : "Probable"
                    : row.has_posted_lineup
                      ? "Posted"
                      : "Projected"}
                </span>
              </div>
              <div className="prediction-confidence">
                <span>{market === "pitcher_strikeouts" ? "Opponent" : "Batting order"}</span>
                <strong>
                  {market === "pitcher_strikeouts"
                    ? row.opponent_team_abbreviation ?? "-"
                    : row.batting_order
                      ? `#${Math.round(row.batting_order)}`
                      : "TBD"}
                </strong>
              </div>
              <div className="prediction-range">
                <div className="prediction-range-fill confidence-mid" style={{ left: "10%", right: "20%" }}></div>
                <span className="prediction-marker">{config.shortLabel}</span>
              </div>
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}

function EvRowTable({ rows, userRegion }: { rows: MlbHrEvRow[]; userRegion: UserRegion }) {
  if (!rows.length) {
    return <p className="text-secondary mt-3 mb-0">No matched HR prices for this view.</p>;
  }

  return (
    <div className="table-responsive mt-3">
      <table className="table align-items-center mb-0 mlb-ev-table">
        <thead>
          <tr>
            <th>Player</th>
            <th>Matchup</th>
            <th className="text-center">Order</th>
            <th className="text-center">Time</th>
            <th className="text-center">Model</th>
            <th className="text-center">FanDuel</th>
            <th className="text-center">Implied</th>
            <th className="text-center">Edge</th>
            <th className="text-center">EV / $1</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={`${row.event_id}-${row.player_id ?? row.player_name}`}>
              <td>
                <div className="d-flex flex-column">
                  <span className="fw-bold text-dark">{row.player_name}</span>
                  <span className="text-xs text-secondary">
                    {row.team_abbreviation ?? "-"} {row.has_posted_lineup ? "posted" : "projected"}
                  </span>
                </div>
              </td>
              <td>
                <span className="text-sm">
                  {row.away_team ?? row.team_abbreviation ?? "-"} @{" "}
                  {row.home_team ?? row.opponent_team_abbreviation ?? "-"}
                </span>
              </td>
              <td className="text-center text-sm">{row.batting_order ? Math.round(row.batting_order) : "-"}</td>
              <td className="text-center text-sm">{formatGameTime(row.commence_time, userRegion)}</td>
              <td className="text-center fw-bold">{formatPct(row.model_probability)}</td>
              <td className="text-center fw-bold">{formatAmerican(row.american_odds)}</td>
              <td className="text-center">{formatPct(row.implied_probability)}</td>
              <td className={`text-center fw-bold ${row.edge > 0 ? "text-success" : "text-danger"}`}>
                {formatPct(row.edge)}
              </td>
              <td className={`text-center fw-bold ${row.ev_per_dollar > 0 ? "text-success" : "text-danger"}`}>
                {formatMoney(row.ev_per_dollar)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ModelStatusGrid({ markets }: { markets: MlbMarketStatus[] }) {
  if (!markets.length) {
    return <p className="text-secondary mt-3">No model status loaded.</p>;
  }

  return (
    <div className="row g-3 mt-1">
      {markets.map((market) => (
        <div className="col-md-6" key={market.market}>
          <div className="mlb-stat-tile h-100">
            <span>{String(market.market).replaceAll("_", " ")}</span>
            <strong>{market.trained ? "Trained" : "Missing"}</strong>
            <p className="text-xs text-secondary mb-0 mt-2">
              Rows {market.rows_total ?? "-"} | Split {market.split_date ?? "-"}
            </p>
          </div>
        </div>
      ))}
    </div>
  );
}

export default function MlbPage() {
  const [userRegion, setUserRegion] = useState<UserRegion>("au");
  const [predictionDay, setPredictionDay] = useState<MlbDay>("auto");
  const [predictionSort, setPredictionSort] = useState<MlbSort>("value_desc");
  const [predictionSearch, setPredictionSearch] = useState("");
  const [predictionTeams, setPredictionTeams] = useState<string[]>([]);
  const [mainTab, setMainTab] = useState<MainTab>("predictions");
  const [market, setMarket] = useState<MlbMarketName>("batter_home_runs");
  const [evView, setEvView] = useState<"positive" | "all">("positive");
  const [bookmaker, setBookmaker] = useState("fanduel");
  const [predictionsState, setPredictionsState] =
    useState<ApiState<MlbPredictionSlateResponse>>(initialPredictionsState);
  const [evState, setEvState] = useState<ApiState<MlbHrEvBoardResponse>>(initialEvState);
  const [evRequestKey, setEvRequestKey] = useState<string | null>(null);
  const [marketStatus, setMarketStatus] = useState<ApiState<{ markets: MlbMarketStatus[] }>>({
    data: null,
    loading: false,
    error: null,
  });
  const [refreshing, setRefreshing] = useState(false);
  const [refreshMessage, setRefreshMessage] = useState<string | null>(null);
  const resolvedMlbDate = resolveMlbSlateDate(predictionDay, userRegion);

  const loadPredictions = async (force = false) => {
    setPredictionsState((current) => ({ ...current, loading: true, error: null }));
    try {
      const data = await getMlbPredictionSlate({
        day: predictionDay,
        date: resolvedMlbDate,
        limit_per_market: 60,
        ensure_data: true,
        refresh: force,
        refresh_key: force ? Date.now() : undefined,
      });
      setPredictionsState({ data, loading: false, error: null });
    } catch (error) {
      setPredictionsState({
        data: null,
        loading: false,
        error: error instanceof Error ? error.message : "Failed to load MLB predictions.",
      });
    }
  };

  const loadEvBoard = async (force = false) => {
    const requestKey = `${resolvedMlbDate}:${bookmaker}`;
    if (!force && evState.data && evRequestKey === requestKey) {
      return;
    }
    setEvState((current) => ({ ...current, loading: true, error: null }));
    try {
      const data = await getMlbHrEvBoard({
        day: predictionDay,
        date: resolvedMlbDate,
        bookmaker,
        max_events: 30,
        prediction_limit: 300,
        limit: 75,
      });
      setEvState({ data, loading: false, error: null });
      setEvRequestKey(requestKey);
    } catch (error) {
      setEvState({
        data: null,
        loading: false,
        error: error instanceof Error ? error.message : "Failed to load MLB EV board.",
      });
    }
  };

  const loadStatus = async () => {
    setMarketStatus((current) => ({ ...current, loading: true, error: null }));
    try {
      const data = await getMlbMarkets();
      setMarketStatus({ data: { markets: data.markets }, loading: false, error: null });
    } catch (error) {
      setMarketStatus({
        data: null,
        loading: false,
        error: error instanceof Error ? error.message : "Failed to load model status.",
      });
    }
  };

  useEffect(() => {
    void loadPredictions();
  }, [resolvedMlbDate]);

  useEffect(() => {
    if (mainTab === "home_run_ev") {
      void loadEvBoard();
    }
  }, [mainTab, resolvedMlbDate, bookmaker]);

  useEffect(() => {
    if (mainTab === "model_status") {
      void loadStatus();
    }
  }, [mainTab]);

  const handleDayChange = (day: MlbDay) => {
    setPredictionDay(day);
    setPredictionTeams([]);
    setPredictionSearch("");
    setEvState(initialEvState);
    setEvRequestKey(null);
  };

  const handleRegionChange = (region: UserRegion) => {
    setUserRegion(region);
    setEvState(initialEvState);
    setEvRequestKey(null);
  };

  const forceRefreshSlate = async () => {
    setRefreshing(true);
    setRefreshMessage(null);
    try {
      await loadPredictions(true);
      if (mainTab === "home_run_ev") {
        await loadEvBoard(true);
      }
      setRefreshMessage("Slate data and predictions refreshed.");
    } catch (error) {
      setRefreshMessage(error instanceof Error ? error.message : "Refresh failed.");
    } finally {
      setRefreshing(false);
    }
  };

  const evRows = evState.data ? (evView === "positive" ? evState.data.positive_ev : evState.data.all) : [];
  const bestEv = evState.data?.positive_ev[0] ?? null;
  const activeMarketRows = predictionsState.data?.markets?.[market]?.data ?? [];
  const activeMarketCount = predictionsState.data?.markets?.[market]?.count ?? 0;
  const activeMissingFeatures =
    predictionsState.data?.markets?.[market]?.missing_model_feature_count ?? 0;
  const normalizedSearch = predictionSearch.trim().toLowerCase();
  const teamOptions = Array.from(
    new Set(
      activeMarketRows
        .map((row) => row.team_abbreviation)
        .filter((team): team is string => Boolean(team))
    )
  ).sort();
  const filteredPredictionRows = sortPredictionRows(
    activeMarketRows.filter((row) => {
      if (predictionTeams.length > 0 && !predictionTeams.includes(row.team_abbreviation ?? "")) {
        return false;
      }
      if (!normalizedSearch) return true;
      return Boolean(
        row.player_name?.toLowerCase().includes(normalizedSearch) ||
          row.team_abbreviation?.toLowerCase().includes(normalizedSearch) ||
          row.opponent_team_abbreviation?.toLowerCase().includes(normalizedSearch)
      );
    }),
    market,
    predictionSort
  );
  const topPrediction = filteredPredictionRows[0] ?? activeMarketRows[0] ?? null;
  const slateDate = predictionsState.data?.date ?? evState.data?.date ?? resolvedMlbDate;
  const dayLabelSuffix = REGION_SHORT[userRegion];
  const evLoadedForCurrentView = Boolean(evState.data && evRequestKey === `${resolvedMlbDate}:${bookmaker}`);

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
                <a className="nav-link nav-link-top text-white opacity-9 px-0 py-1" href="/nba">
                  NBA
                </a>
                <span className="nav-separator">|</span>
                <a className="nav-link nav-link-top text-white opacity-9 px-0 py-1" href="/mlb">
                  MLB
                </a>
                <span className="nav-separator">|</span>
                <a className="nav-link nav-link-top text-white opacity-9 px-0 py-1" href="/performance">
                  How We&apos;re Doing
                </a>
              </div>
              <div className="d-flex align-items-center gap-2">
                <label className="text-xs text-white opacity-8 mb-0" htmlFor="mlb-region-select">
                  Region
                </label>
                <select
                  id="mlb-region-select"
                  className="form-select form-select-sm region-select"
                  value={userRegion}
                  onChange={(event) => handleRegionChange(event.target.value as UserRegion)}
                >
                  <option value="au">Australia</option>
                  <option value="us">USA</option>
                  <option value="uk">England</option>
                </select>
              </div>
            </div>
          </nav>

          <div className="row">
            <div className="col-lg-8 col-xl-7">
              <div className="hero-copy">
                <p className="hero-slogan mb-3">MLB</p>
                <h1 className="display-4 text-white mb-3">Baseball prediction hub.</h1>
                <p className="lead text-white opacity-8 mb-4">
                  Home runs, hits, total bases, pitcher strikeouts, and FanDuel HR value on one MLB slate.
                </p>
                <p className="text-sm text-white opacity-8 mb-0">
                  Dates use the official MLB slate date. Times display in the selected region.
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
                <div className="section-header d-flex flex-wrap align-items-center justify-content-between gap-3">
                  <div>
                    <h3 className="mb-1">MLB Hub</h3>
                    <p className="text-secondary mb-0">
                      {mainTab === "predictions"
                        ? `${MARKET_CONFIG[market].label} for ${slateDate}`
                        : mainTab === "home_run_ev"
                          ? `FanDuel HR value for ${slateDate}`
                          : "Model and data status"}
                    </p>
                  </div>
                  <span className="badge badge-sm bg-gradient-light text-dark">
                    Official MLB date
                  </span>
                </div>

                <div className="nav-wrapper position-relative mt-4">
                  <ul className="nav nav-pills flex-wrap p-1 tab-pills" role="tablist">
                    <li className="nav-item">
                      <a
                        className={`nav-link mb-0 px-0 py-1 ${mainTab === "predictions" ? "active" : ""}`}
                        onClick={() => setMainTab("predictions")}
                        role="tab"
                        style={{ cursor: "pointer" }}
                      >
                        <i className="material-symbols-rounded me-2">psychology</i>
                        Predictions
                      </a>
                    </li>
                    <li className="nav-item">
                      <a
                        className={`nav-link mb-0 px-0 py-1 ${mainTab === "home_run_ev" ? "active" : ""}`}
                        onClick={() => setMainTab("home_run_ev")}
                        role="tab"
                        style={{ cursor: "pointer" }}
                      >
                        <i className="material-symbols-rounded me-2">paid</i>
                        HR EV
                      </a>
                    </li>
                    <li className="nav-item">
                      <a
                        className={`nav-link mb-0 px-0 py-1 ${mainTab === "model_status" ? "active" : ""}`}
                        onClick={() => setMainTab("model_status")}
                        role="tab"
                        style={{ cursor: "pointer" }}
                      >
                        <i className="material-symbols-rounded me-2">query_stats</i>
                        Model Status
                      </a>
                    </li>
                  </ul>
                </div>

                {refreshMessage && <div className="alert alert-light border text-sm py-2 mt-3">{refreshMessage}</div>}

                {mainTab === "predictions" && (
                  <>
                    <div className="d-flex flex-column flex-lg-row justify-content-between align-items-start gap-3 mt-4 mb-4">
                      <div>
                        <h4 className="mb-1">Predictions</h4>
                        <p className="text-xs text-secondary mb-0">
                          {DAY_OPTIONS.find((option) => option.value === predictionDay)?.label} ({dayLabelSuffix}) maps to MLB/US slate {resolvedMlbDate}.
                        </p>
                      </div>
                      <div className="d-flex flex-wrap gap-2 align-items-center">
                        <div className="stat-toggle">
                          {(Object.keys(MARKET_CONFIG) as MlbMarketName[]).map((key) => (
                            <button
                              key={key}
                              className={`stat-chip ${market === key ? "active" : ""}`}
                              type="button"
                              onClick={() => setMarket(key)}
                            >
                              {MARKET_CONFIG[key].tabLabel}
                            </button>
                          ))}
                        </div>
                        <div className="prediction-select-group">
                          <label className="prediction-select-field">
                            <span className="prediction-select-label">Day</span>
                            <select
                              className="form-select form-select-sm"
                              value={predictionDay}
                              aria-label="MLB prediction day"
                              onChange={(event) => handleDayChange(event.target.value as MlbDay)}
                            >
                              {DAY_OPTIONS.map((option) => (
                                <option key={option.value} value={option.value}>
                                  {option.label} ({dayLabelSuffix})
                                </option>
                              ))}
                            </select>
                          </label>
                          <label className="prediction-select-field">
                            <span className="prediction-select-label">Sort</span>
                            <select
                              className="form-select form-select-sm"
                              value={predictionSort}
                              aria-label="MLB prediction sort order"
                              onChange={(event) => setPredictionSort(event.target.value as MlbSort)}
                            >
                              {SORT_OPTIONS.map((option) => (
                                <option key={option.value} value={option.value}>
                                  {option.label}
                                </option>
                              ))}
                            </select>
                          </label>
                        </div>
                        <button
                          className="btn btn-sm bg-gradient-primary mb-0"
                          type="button"
                          onClick={() => void loadPredictions()}
                          disabled={predictionsState.loading}
                        >
                          {predictionsState.loading ? (
                            <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                          ) : (
                            <i className="material-symbols-rounded me-2" style={{ fontSize: "16px" }}>
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
                          onChange={(event) => setPredictionSearch(event.target.value)}
                        />
                      </div>
                      <div className="prediction-team-chips">
                        <button
                          className={`team-chip ${predictionTeams.length === 0 ? "active" : ""}`}
                          type="button"
                          onClick={() => setPredictionTeams([])}
                        >
                          All teams
                        </button>
                        {teamOptions.map((team) => (
                          <button
                            key={team}
                            className={`team-chip ${predictionTeams.includes(team) ? "active" : ""}`}
                            type="button"
                            onClick={() =>
                              setPredictionTeams((current) =>
                                current.includes(team)
                                  ? current.filter((selected) => selected !== team)
                                  : [...current, team]
                              )
                            }
                          >
                            {team}
                          </button>
                        ))}
                      </div>
                    </div>
                    <div className="d-flex justify-content-between align-items-center gap-3">
                      <span className="text-xs text-secondary">
                        {filteredPredictionRows.length} shown from {activeMarketCount} rows
                        {predictionsState.data?.source ? ` | ${predictionsState.data.source}` : ""}
                      </span>
                      <span className="text-xs text-secondary">US slate {resolvedMlbDate} | Missing features {activeMissingFeatures}</span>
                    </div>
                    {predictionsState.error && <div className="alert alert-danger text-sm mt-3">{predictionsState.error}</div>}
                    {predictionsState.loading && <p className="text-secondary mt-3">Loading MLB predictions...</p>}
                    {!predictionsState.loading && (
                      <MlbPredictionsGrid rows={filteredPredictionRows} market={market} userRegion={userRegion} />
                    )}
                  </>
                )}

                {mainTab === "home_run_ev" && (
                  <>
                    <div className="d-flex flex-wrap align-items-end justify-content-between mt-4 gap-3">
                      <div className="best-bets-controls">
                        <div className="control-group">
                          <label className="form-label" htmlFor="mlb-ev-day">
                            Day
                          </label>
                          <select
                            id="mlb-ev-day"
                            className="form-select form-select-sm"
                            value={predictionDay}
                            onChange={(event) => handleDayChange(event.target.value as MlbDay)}
                          >
                            {DAY_OPTIONS.map((option) => (
                              <option key={option.value} value={option.value}>
                                {option.label} ({dayLabelSuffix})
                              </option>
                            ))}
                          </select>
                        </div>
                        <div className="control-group">
                          <label className="form-label" htmlFor="mlb-bookie">
                            Book
                          </label>
                          <select
                            id="mlb-bookie"
                            className="form-select form-select-sm"
                            value={bookmaker}
                            onChange={(event) => {
                              setBookmaker(event.target.value);
                              setEvState(initialEvState);
                              setEvRequestKey(null);
                            }}
                          >
                            <option value="fanduel">FanDuel</option>
                          </select>
                        </div>
                        <div className="control-group">
                          <label className="form-label" htmlFor="mlb-view">
                            View
                          </label>
                          <select
                            id="mlb-view"
                            className="form-select form-select-sm"
                            value={evView}
                            onChange={(event) => setEvView(event.target.value as "positive" | "all")}
                          >
                            <option value="positive">Positive EV</option>
                            <option value="all">All matched</option>
                          </select>
                        </div>
                        <button
                          className="btn btn-sm bg-gradient-primary mb-0 align-self-end"
                          type="button"
                          onClick={() => void loadEvBoard()}
                          disabled={evState.loading || evLoadedForCurrentView}
                        >
                          {evState.loading ? "Loading..." : evLoadedForCurrentView ? "Loaded" : "Load HR EV"}
                        </button>
                      </div>
                      <span className="text-xs text-secondary">
                        {evState.data?.matched ?? 0} matched | {evState.data?.props_count ?? 0} props
                      </span>
                    </div>
                    <p className="text-xs text-secondary mt-2 mb-0">
                      PropLine is only called from this tab to protect the free request limit.
                    </p>
                    {evState.error && <div className="alert alert-danger text-sm mt-3">{evState.error}</div>}
                    {evState.loading && <p className="text-secondary mt-3">Loading MLB EV board...</p>}
                    {!evState.loading && <EvRowTable rows={evRows.slice(0, 40)} userRegion={userRegion} />}
                  </>
                )}

                {mainTab === "model_status" && (
                  <>
                    {marketStatus.error && <div className="alert alert-danger text-sm mt-3">{marketStatus.error}</div>}
                    {marketStatus.loading && <p className="text-secondary mt-3">Loading model status...</p>}
                    {!marketStatus.loading && <ModelStatusGrid markets={marketStatus.data?.markets ?? []} />}
                  </>
                )}
              </div>
            </div>

            <div className="col-lg-4">
              <div className="section-card mb-4 prediction-focus">
                <p className="text-uppercase text-xs text-secondary fw-bold mb-2">Slate Snapshot</p>
                <h4 className="mb-3">{slateDate}</h4>
                <div className="row g-3">
                  <div className="col-6">
                    <div className="mlb-stat-tile">
                      <span>Predictions</span>
                      <strong>{activeMarketCount}</strong>
                    </div>
                  </div>
                  <div className="col-6">
                    <div className="mlb-stat-tile">
                      <span>Positive EV</span>
                      <strong>{evState.data?.positive_ev.length ?? 0}</strong>
                    </div>
                  </div>
                </div>
                <div className="border-top pt-3 mt-3">
                  <p className="text-xs text-secondary fw-bold text-uppercase mb-2">Top card</p>
                  <h5 className="mb-1">{topPrediction?.player_name ?? "-"}</h5>
                  <p className="text-sm text-secondary mb-0">
                    {topPrediction ? `${MARKET_CONFIG[market].label}: ${formatMarketValue(topPrediction, market)}` : "No prediction loaded"}
                  </p>
                </div>
                <div className="border-top pt-3 mt-3">
                  <p className="text-xs text-secondary fw-bold text-uppercase mb-2">Best HR EV</p>
                  <h5 className="mb-1">{bestEv?.player_name ?? "-"}</h5>
                  <p className="text-sm text-secondary mb-0">
                    {bestEv ? `${formatAmerican(bestEv.american_odds)} | ${formatMoney(bestEv.ev_per_dollar)} per $1` : "Open HR EV tab to load"}
                  </p>
                </div>
              </div>

              <div className="section-card">
                <p className="text-uppercase text-xs text-secondary fw-bold mb-2">Data</p>
                <h4 className="mb-2">Quick refresh</h4>
                <p className="text-sm text-secondary">
                  The page auto-loads missing schedule and roster data. Use this only to force a fresh slate pull.
                </p>
                <button
                  className="btn btn-outline-dark btn-sm mb-0"
                  type="button"
                  onClick={() => void forceRefreshSlate()}
                  disabled={refreshing}
                >
                  {refreshing ? "Refreshing..." : "Force refresh"}
                </button>
              </div>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}
