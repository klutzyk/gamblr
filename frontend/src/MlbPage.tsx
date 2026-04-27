import { useEffect, useState } from "react";
import "./App.css";
import {
  getMlbHrEvBoard,
  getMlbPredictionSlate,
  getMlbSimulationGames,
  runMlbGameSimulation,
  type MlbHrEvBoardResponse,
  type MlbHrEvRow,
  type MlbMarketName,
  type MlbPredictionSlateResponse,
  type MlbPredictionRow,
  type MlbSimulationFieldEvent,
  type MlbSimulationGame,
  type MlbSimulationGamesResponse,
  type MlbSimulationRunResponse,
} from "./api";
import logo from "./assets/logo2.png";

type UserRegion = "au" | "us" | "uk";
type MainTab = "predictions" | "home_run_ev" | "simulation";
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

const initialSimulationGamesState: ApiState<MlbSimulationGamesResponse> = {
  data: null,
  loading: false,
  error: null,
};

const initialSimulationRunState: ApiState<MlbSimulationRunResponse> = {
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

function formatNumber(value?: number | null, digits = 1) {
  if (typeof value !== "number" || Number.isNaN(value)) return "-";
  return value.toFixed(digits);
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

function getPlayerStatsSearchUrl(playerName: string | null | undefined): string {
  const safeName =
    typeof playerName === "string" && playerName.trim().length > 0
      ? playerName.trim()
      : "MLB player";
  const url = new URL("https://www.google.com/search");
  url.searchParams.set("q", `${safeName} MLB stats`);
  return url.toString();
}

function getMlbHeadshotUrl(playerId: number | null | undefined) {
  if (!playerId) return "";
  return `https://img.mlbstatic.com/mlb-photos/image/upload/w_213,q_100/v1/people/${playerId}/headshot/67/current`;
}

function getVenueText(row: MlbPredictionRow) {
  if (!row.venue_name) return null;
  const location = [row.venue_city, row.venue_state].filter(Boolean).join(", ");
  return location ? `${row.venue_name}, ${location}` : row.venue_name;
}

function formatWeatherContext(row: MlbPredictionRow) {
  const tempF =
    typeof row.temperature_f === "number"
      ? row.temperature_f
      : typeof row.temperature_2m_c === "number"
        ? row.temperature_2m_c * 1.8 + 32
        : null;
  const windMph =
    typeof row.wind_speed_10m_kph === "number" ? row.wind_speed_10m_kph * 0.621371 : null;
  const parts = [
    typeof tempF === "number" ? `${Math.round(tempF)}F` : null,
    row.wind_text || (typeof windMph === "number" ? `Wind ${Math.round(windMph)} mph` : null),
    row.roof_type ? row.roof_type : null,
  ].filter(Boolean);
  return parts.length ? parts.join(" / ") : null;
}

function normalizeRecentGames(value: MlbPredictionRow["recent_games"]) {
  if (!value) return [];
  if (Array.isArray(value)) return value;
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }
  return [];
}

function recentStatKeyForMarket(market: MlbMarketName) {
  if (market === "batter_home_runs") return "home_runs";
  if (market === "batter_hits") return "hits";
  if (market === "batter_total_bases") return "total_bases";
  return "strikeouts";
}

function recentStatLabelForMarket(market: MlbMarketName) {
  if (market === "batter_home_runs") return "HR";
  if (market === "batter_hits") return "H";
  if (market === "batter_total_bases") return "TB";
  return "K";
}

function recentValuesForMarket(row: MlbPredictionRow, market: MlbMarketName) {
  const key = recentStatKeyForMarket(market);
  return normalizeRecentGames(row.recent_games)
    .slice(0, 5)
    .map((game) => Number(game[key] ?? 0));
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
      {rows.map((row) => {
        const headshotUrl = getMlbHeadshotUrl(row.player_id);
        const venue = getVenueText(row);
        const weather = formatWeatherContext(row);
        const recentValues = recentValuesForMarket(row, market);
        const recentLabel = recentStatLabelForMarket(market);
        return (
        <div key={`${market}-${row.game_pk}-${row.player_id}`} className="card card-body border-radius-xl shadow-lg">
          <div className="d-flex justify-content-between align-items-start mb-3">
            <div className="flex-grow-1">
              <div className="d-flex align-items-center gap-2 mb-1">
                <div className="player-avatar mlb-player-avatar">
                  {headshotUrl && (
                    <img
                      src={headshotUrl}
                      alt={row.player_name}
                      onError={(event) => {
                        event.currentTarget.style.display = "none";
                      }}
                    />
                  )}
                  <span className="avatar-fallback">{getInitials(row.player_name)}</span>
                </div>
                <div>
                  <h5 className="mb-1">
                    <a
                      href={getPlayerStatsSearchUrl(row.player_name)}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="player-name-link"
                    >
                      {row.player_name}
                    </a>
                  </h5>
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
            {venue && (
              <div className="d-flex align-items-center mb-2">
                <i className="material-symbols-rounded text-secondary me-2">location_on</i>
                <span className="text-sm text-secondary">{venue}</span>
              </div>
            )}
            {weather && (
              <div className="d-flex align-items-center mb-2">
                <i className="material-symbols-rounded text-secondary me-2">partly_cloudy_day</i>
                <span className="text-sm text-secondary">{weather}</span>
              </div>
            )}
            <div className="prediction-stat-tag mt-3">
              <span className="badge badge-sm bg-gradient-info">{config.label}</span>
            </div>
            {recentValues.length > 0 && (
              <div className="mlb-recent-form mt-3">
                <span>Last 5 {recentLabel}</span>
                <div>
                  {recentValues.map((value, index) => (
                    <strong key={`${row.player_id}-${market}-recent-${index}`}>{value}</strong>
                  ))}
                </div>
              </div>
            )}

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
      );
      })}
    </div>
  );
}

function EvRowTable({ rows, userRegion }: { rows: MlbHrEvRow[]; userRegion: UserRegion }) {
  if (!rows.length) {
    return <p className="text-secondary mt-3 mb-0">No home run prices are available for this slate.</p>;
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
            <th className="text-center">Projected</th>
            <th className="text-center">Odds</th>
            <th className="text-center">Book Chance</th>
            <th className="text-center">Edge</th>
            <th className="text-center">Value / $1</th>
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

function simulationGameLabel(game: MlbSimulationGame, region: UserRegion) {
  return `${game.away_abbreviation} @ ${game.home_abbreviation} / ${formatGameTime(game.start_time_utc, region)}`;
}

function formatSimulationResult(value: string | null | undefined) {
  if (!value) return "-";
  return value
    .split("_")
    .map((part) => part.slice(0, 1).toUpperCase() + part.slice(1))
    .join(" ");
}

function recordNumber(row: Record<string, number | string | null>, key: string, digits = 2) {
  const value = row[key];
  return typeof value === "number" && Number.isFinite(value) ? value.toFixed(digits) : "-";
}

function recordText(row: Record<string, number | string | null>, key: string) {
  const value = row[key];
  return typeof value === "string" && value.length > 0 ? value : "-";
}

function SimulationField({
  events,
  activeIndex,
}: {
  events: MlbSimulationFieldEvent[];
  activeIndex: number;
}) {
  const activeEvent = events[activeIndex] ?? null;
  const recentEvents = events.slice(Math.max(0, activeIndex - 24), activeIndex + 1);
  return (
    <div className="simulation-field-wrap">
      <svg className="simulation-field" viewBox="0 0 100 100" role="img" aria-label="Simulated batted ball field">
        <defs>
          <linearGradient id="fieldGrass" x1="0" x2="1" y1="0" y2="1">
            <stop offset="0%" stopColor="#163f2b" />
            <stop offset="100%" stopColor="#0c241b" />
          </linearGradient>
          <linearGradient id="infieldClay" x1="0" x2="1" y1="0" y2="1">
            <stop offset="0%" stopColor="#c79052" />
            <stop offset="100%" stopColor="#8e5a32" />
          </linearGradient>
        </defs>
        <path d="M6 86 Q50 7 94 86 Z" fill="url(#fieldGrass)" />
        <path d="M15 84 Q50 24 85 84" fill="none" stroke="rgba(255,255,255,0.55)" strokeWidth="0.8" />
        <path d="M50 92 L31 75 L50 59 L69 75 Z" fill="url(#infieldClay)" stroke="rgba(255,255,255,0.55)" strokeWidth="0.7" />
        <path d="M50 92 L31 75 M50 92 L69 75 M31 75 L50 59 M69 75 L50 59" stroke="rgba(255,255,255,0.42)" strokeWidth="0.7" />
        <circle cx="50" cy="92" r="1.2" fill="#f8f1dd" />
        <circle cx="31" cy="75" r="1.1" fill="#f8f1dd" />
        <circle cx="50" cy="59" r="1.1" fill="#f8f1dd" />
        <circle cx="69" cy="75" r="1.1" fill="#f8f1dd" />
        {recentEvents.map((event, index) => (
          <circle
            key={`${event.pitch_number}-${index}`}
            cx={event.field_x}
            cy={event.field_y}
            r={index === recentEvents.length - 1 ? 1.4 : 0.7}
            fill={index === recentEvents.length - 1 ? "#f6e55b" : "rgba(246,229,91,0.42)"}
          />
        ))}
        {activeEvent && (
          <>
            <path
              d={`M50 92 Q${(50 + activeEvent.field_x) / 2} ${Math.min(52, activeEvent.field_y + 12)} ${activeEvent.field_x} ${activeEvent.field_y}`}
              fill="none"
              stroke="#f6e55b"
              strokeWidth="1.15"
              strokeLinecap="round"
            />
            <circle cx={activeEvent.field_x} cy={activeEvent.field_y} r="2.1" fill="#ffffff" stroke="#f6e55b" strokeWidth="1" />
          </>
        )}
      </svg>
      <div className="simulation-field-readout">
        <span>{activeEvent ? `Pitch ${activeEvent.pitch_number}` : "No contact"}</span>
        <strong>{activeEvent ? formatSimulationResult(activeEvent.result) : "Run a simulation"}</strong>
        <small>
          {activeEvent
            ? `${formatNumber(activeEvent.distance_ft, 0)} ft / ${formatNumber(activeEvent.launch_speed, 1)} mph / ${formatNumber(activeEvent.launch_angle, 0)} deg`
            : "Batted balls will render here"}
        </small>
      </div>
    </div>
  );
}

function SimulationResults({
  result,
  activeEventIndex,
  onEventIndexChange,
}: {
  result: MlbSimulationRunResponse;
  activeEventIndex: number;
  onEventIndexChange: (index: number) => void;
}) {
  const events = result.sample.field_events;
  const maxEventIndex = Math.max(events.length - 1, 0);
  const activeIndex = Math.min(activeEventIndex, maxEventIndex);
  const pitchRows = result.sample.pitch_log.slice(0, 90);
  return (
    <div className="simulation-results mt-4">
      <div className="simulation-scoreboard">
        <div>
          <span>{result.game.away_abbreviation}</span>
          <strong>{formatPct(result.summary.away_win_probability, 1)}</strong>
          <small>{formatNumber(result.summary.away_avg_score, 2)} runs</small>
        </div>
        <div className="simulation-score-divider">
          <span>{result.iterations} sims</span>
          <strong>
            {result.summary.sample_score?.away ?? "-"}-{result.summary.sample_score?.home ?? "-"}
          </strong>
          <small>{formatNumber(result.summary.avg_pitch_count, 0)} pitches</small>
        </div>
        <div>
          <span>{result.game.home_abbreviation}</span>
          <strong>{formatPct(result.summary.home_win_probability, 1)}</strong>
          <small>{formatNumber(result.summary.home_avg_score, 2)} runs</small>
        </div>
      </div>

      <div className="simulation-meta-grid mt-3">
        <div>
          <span>Away starter</span>
          <strong>{result.inputs.away_starter ?? "-"}</strong>
        </div>
        <div>
          <span>Home starter</span>
          <strong>{result.inputs.home_starter ?? "-"}</strong>
        </div>
        <div>
          <span>Weather</span>
          <strong>{result.inputs.weather_mode ?? "-"}</strong>
        </div>
        <div>
          <span>Umpire</span>
          <strong>{String(result.game.home_plate_umpire?.full_name ?? "-")}</strong>
        </div>
      </div>

      <div className="row g-4 mt-1">
        <div className="col-lg-5">
          <SimulationField events={events} activeIndex={activeIndex} />
          {events.length > 0 && (
            <div className="simulation-scrub mt-3">
              <button
                type="button"
                className="btn btn-sm btn-outline-dark mb-0"
                onClick={() => onEventIndexChange(Math.max(0, activeIndex - 1))}
              >
                <i className="material-symbols-rounded">chevron_left</i>
              </button>
              <input
                type="range"
                min={0}
                max={maxEventIndex}
                value={activeIndex}
                onChange={(event) => onEventIndexChange(Number(event.target.value))}
              />
              <button
                type="button"
                className="btn btn-sm btn-outline-dark mb-0"
                onClick={() => onEventIndexChange(Math.min(maxEventIndex, activeIndex + 1))}
              >
                <i className="material-symbols-rounded">chevron_right</i>
              </button>
            </div>
          )}
        </div>
        <div className="col-lg-7">
          <div className="table-responsive simulation-table-wrap">
            <table className="table align-items-center mb-0 mlb-ev-table">
              <thead>
                <tr>
                  <th>Hitter</th>
                  <th className="text-center">Team</th>
                  <th className="text-center">HR %</th>
                  <th className="text-center">Hits</th>
                  <th className="text-center">TB</th>
                  <th className="text-center">RBI</th>
                </tr>
              </thead>
              <tbody>
                {result.top_batters.slice(0, 10).map((row) => (
                  <tr key={String(row.player_id)}>
                    <td className="fw-bold">{recordText(row, "name")}</td>
                    <td className="text-center">{recordText(row, "team")}</td>
                    <td className="text-center fw-bold">{formatPct(typeof row.home_run_probability === "number" ? row.home_run_probability : null)}</td>
                    <td className="text-center">{recordNumber(row, "avg_hits", 3)}</td>
                    <td className="text-center">{recordNumber(row, "avg_total_bases", 3)}</td>
                    <td className="text-center">{recordNumber(row, "avg_rbi", 3)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className="row g-4 mt-1">
        <div className="col-lg-5">
          <div className="table-responsive simulation-table-wrap">
            <table className="table align-items-center mb-0 mlb-ev-table">
              <thead>
                <tr>
                  <th>Pitcher</th>
                  <th className="text-center">K</th>
                  <th className="text-center">Pitches</th>
                  <th className="text-center">Runs</th>
                </tr>
              </thead>
              <tbody>
                {result.pitchers.slice(0, 6).map((row) => (
                  <tr key={String(row.player_id)}>
                    <td className="fw-bold">{recordText(row, "name")}</td>
                    <td className="text-center">{recordNumber(row, "avg_strikeouts", 2)}</td>
                    <td className="text-center">{recordNumber(row, "avg_pitches", 1)}</td>
                    <td className="text-center">{recordNumber(row, "avg_runs_allowed", 2)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
        <div className="col-lg-7">
          <div className="table-responsive simulation-table-wrap simulation-pitch-log">
            <table className="table align-items-center mb-0 mlb-ev-table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Count</th>
                  <th>Batter</th>
                  <th>Pitch</th>
                  <th>Call</th>
                  <th>Score</th>
                </tr>
              </thead>
              <tbody>
                {pitchRows.map((row) => (
                  <tr key={row.pitch_number}>
                    <td>{row.pitch_number}</td>
                    <td>
                      {row.balls_before}-{row.strikes_before}
                    </td>
                    <td className="fw-bold">{row.batter}</td>
                    <td>
                      {row.pitch_type} {formatNumber(row.pitch_mph, 1)}
                    </td>
                    <td>{formatSimulationResult(row.result ?? row.call)}</td>
                    <td>{row.score}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
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
  const [simulationGamesState, setSimulationGamesState] =
    useState<ApiState<MlbSimulationGamesResponse>>(initialSimulationGamesState);
  const [simulationRunState, setSimulationRunState] =
    useState<ApiState<MlbSimulationRunResponse>>(initialSimulationRunState);
  const [evRequestKey, setEvRequestKey] = useState<string | null>(null);
  const [selectedSimulationGamePk, setSelectedSimulationGamePk] = useState<number | null>(null);
  const [simulationIterations, setSimulationIterations] = useState(250);
  const [simulationSeed, setSimulationSeed] = useState("");
  const [simulationEventIndex, setSimulationEventIndex] = useState(0);
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
        max_age_minutes: 30,
        refresh: force,
        refresh_key: force ? Date.now() : undefined,
        prediction_limit: 300,
        limit: 75,
      });
      setEvState({ data, loading: false, error: null });
      setEvRequestKey(requestKey);
    } catch (error) {
      setEvState({
        data: null,
        loading: false,
        error: error instanceof Error ? error.message : "Failed to load home run prices.",
      });
    }
  };

  const resetSimulation = () => {
    setSimulationGamesState(initialSimulationGamesState);
    setSimulationRunState(initialSimulationRunState);
    setSelectedSimulationGamePk(null);
    setSimulationEventIndex(0);
  };

  const loadSimulationGames = async () => {
    setSimulationGamesState((current) => ({ ...current, loading: true, error: null }));
    try {
      const data = await getMlbSimulationGames({ date: resolvedMlbDate });
      setSimulationGamesState({ data, loading: false, error: null });
      setSelectedSimulationGamePk((current) =>
        current && data.games.some((game) => game.game_pk === current)
          ? current
          : data.games[0]?.game_pk ?? null
      );
    } catch (error) {
      setSimulationGamesState({
        data: null,
        loading: false,
        error: error instanceof Error ? error.message : "Failed to load MLB simulation games.",
      });
    }
  };

  const runSelectedSimulation = async () => {
    if (!selectedSimulationGamePk) return;
    setSimulationRunState((current) => ({ ...current, loading: true, error: null }));
    setSimulationEventIndex(0);
    try {
      const parsedSeed = simulationSeed.trim() ? Number(simulationSeed.trim()) : undefined;
      const data = await runMlbGameSimulation({
        game_pk: selectedSimulationGamePk,
        iterations: simulationIterations,
        seed: Number.isFinite(parsedSeed) ? parsedSeed : undefined,
        pitch_log_limit: 900,
      });
      setSimulationRunState({ data, loading: false, error: null });
    } catch (error) {
      setSimulationRunState({
        data: null,
        loading: false,
        error: error instanceof Error ? error.message : "Failed to run MLB simulation.",
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
    if (mainTab === "simulation") {
      void loadSimulationGames();
    }
  }, [mainTab, resolvedMlbDate]);

  const handleDayChange = (day: MlbDay) => {
    setPredictionDay(day);
    setPredictionTeams([]);
    setPredictionSearch("");
    setEvState(initialEvState);
    setEvRequestKey(null);
    resetSimulation();
  };

  const handleRegionChange = (region: UserRegion) => {
    setUserRegion(region);
    setEvState(initialEvState);
    setEvRequestKey(null);
    resetSimulation();
  };

  const evRows = evState.data ? (evView === "positive" ? evState.data.positive_ev : evState.data.all) : [];
  const bestEv = evState.data?.positive_ev[0] ?? null;
  const activeMarketRows = predictionsState.data?.markets?.[market]?.data ?? [];
  const activeMarketCount = predictionsState.data?.markets?.[market]?.count ?? 0;
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
  const simulationGames = simulationGamesState.data?.games ?? [];
  const selectedSimulationGame = simulationGames.find((game) => game.game_pk === selectedSimulationGamePk) ?? null;
  const dayLabelSuffix = REGION_SHORT[userRegion];
  const selectedDayLabel = DAY_OPTIONS.find((option) => option.value === predictionDay)?.label ?? "Slate";
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
                <h1 className="display-4 text-white mb-3">MLB betting dashboard</h1>
                <p className="lead text-white opacity-8 mb-4">
                  Player projections, matchup context, and home run value in one place.
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
                        ? MARKET_CONFIG[market].label
                        : mainTab === "home_run_ev"
                          ? "Home Run Value"
                          : "Pitch Simulation"}
                    </p>
                  </div>
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
                        Home Run Value
                      </a>
                    </li>
                    <li className="nav-item">
                      <a
                        className={`nav-link mb-0 px-0 py-1 ${mainTab === "simulation" ? "active" : ""}`}
                        onClick={() => setMainTab("simulation")}
                        role="tab"
                        style={{ cursor: "pointer" }}
                      >
                        <i className="material-symbols-rounded me-2">route</i>
                        Simulation
                      </a>
                    </li>
                  </ul>
                </div>

                {mainTab === "predictions" && (
                  <>
                    <div className="d-flex flex-column flex-lg-row justify-content-between align-items-start gap-3 mt-4 mb-4">
                      <div>
                        <h4 className="mb-1">Predictions</h4>
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
                      <span className="text-xs text-secondary">{filteredPredictionRows.length} players</span>
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
                            <option value="positive">Best Value</option>
                            <option value="all">All Prices</option>
                          </select>
                        </div>
                        <button
                          className="btn btn-sm bg-gradient-primary mb-0 align-self-end"
                          type="button"
                          onClick={() => void loadEvBoard()}
                          disabled={evState.loading || evLoadedForCurrentView}
                        >
                          {evState.loading ? "Loading..." : evLoadedForCurrentView ? "Prices Loaded" : "Load Prices"}
                        </button>
                      </div>
                      <span className="text-xs text-secondary">
                        {evState.data?.matched ?? 0} players priced
                      </span>
                    </div>
                    {evState.error && <div className="alert alert-danger text-sm mt-3">{evState.error}</div>}
                    {evState.loading && <p className="text-secondary mt-3">Loading home run prices...</p>}
                    {!evState.loading && <EvRowTable rows={evRows.slice(0, 40)} userRegion={userRegion} />}
                  </>
                )}

                {mainTab === "simulation" && (
                  <>
                    <div className="d-flex flex-wrap align-items-end justify-content-between mt-4 gap-3">
                      <div>
                        <h4 className="mb-1">Pitch Simulation</h4>
                        <p className="text-secondary mb-0">
                          {selectedSimulationGame
                            ? `${selectedSimulationGame.away_abbreviation} @ ${selectedSimulationGame.home_abbreviation}`
                            : "Select a game"}
                        </p>
                      </div>
                      <div className="best-bets-controls simulation-controls">
                        <div className="control-group">
                          <label className="form-label" htmlFor="mlb-sim-day">
                            Day
                          </label>
                          <select
                            id="mlb-sim-day"
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
                        <div className="control-group simulation-game-select">
                          <label className="form-label" htmlFor="mlb-sim-game">
                            Game
                          </label>
                          <select
                            id="mlb-sim-game"
                            className="form-select form-select-sm"
                            value={selectedSimulationGamePk ?? ""}
                            onChange={(event) => {
                              setSelectedSimulationGamePk(Number(event.target.value));
                              setSimulationRunState(initialSimulationRunState);
                              setSimulationEventIndex(0);
                            }}
                          >
                            {simulationGames.length === 0 && <option value="">No games</option>}
                            {simulationGames.map((game) => (
                              <option key={game.game_pk} value={game.game_pk}>
                                {simulationGameLabel(game, userRegion)}
                              </option>
                            ))}
                          </select>
                        </div>
                        <div className="control-group">
                          <label className="form-label" htmlFor="mlb-sim-iterations">
                            Sims
                          </label>
                          <input
                            id="mlb-sim-iterations"
                            className="form-control form-control-sm"
                            type="number"
                            min={1}
                            max={2000}
                            step={50}
                            value={simulationIterations}
                            onChange={(event) =>
                              setSimulationIterations(Math.max(1, Math.min(2000, Number(event.target.value) || 1)))
                            }
                          />
                        </div>
                        <div className="control-group">
                          <label className="form-label" htmlFor="mlb-sim-seed">
                            Seed
                          </label>
                          <input
                            id="mlb-sim-seed"
                            className="form-control form-control-sm"
                            type="number"
                            placeholder="Auto"
                            value={simulationSeed}
                            onChange={(event) => setSimulationSeed(event.target.value)}
                          />
                        </div>
                        <button
                          className="btn btn-sm bg-gradient-primary mb-0 align-self-end"
                          type="button"
                          onClick={() => void runSelectedSimulation()}
                          disabled={simulationRunState.loading || !selectedSimulationGamePk}
                        >
                          {simulationRunState.loading ? "Running..." : "Run Simulation"}
                        </button>
                      </div>
                    </div>
                    {simulationGamesState.loading && <p className="text-secondary mt-3">Loading simulation slate...</p>}
                    {simulationGamesState.error && <div className="alert alert-danger text-sm mt-3">{simulationGamesState.error}</div>}
                    {selectedSimulationGame && (
                      <div className="simulation-game-strip mt-4">
                        <div>
                          <span>Venue</span>
                          <strong>{selectedSimulationGame.venue_name ?? "-"}</strong>
                        </div>
                        <div>
                          <span>Weather rows</span>
                          <strong>{selectedSimulationGame.weather_rows ?? 0}</strong>
                        </div>
                        <div>
                          <span>Lineups</span>
                          <strong>
                            {selectedSimulationGame.away_lineup_count ?? 0}/{selectedSimulationGame.home_lineup_count ?? 0}
                          </strong>
                        </div>
                        <div>
                          <span>Starters</span>
                          <strong>
                            {selectedSimulationGame.away_pitcher ?? "TBD"} / {selectedSimulationGame.home_pitcher ?? "TBD"}
                          </strong>
                        </div>
                      </div>
                    )}
                    {simulationRunState.error && <div className="alert alert-danger text-sm mt-3">{simulationRunState.error}</div>}
                    {simulationRunState.data && (
                      <SimulationResults
                        result={simulationRunState.data}
                        activeEventIndex={simulationEventIndex}
                        onEventIndexChange={setSimulationEventIndex}
                      />
                    )}
                  </>
                )}
              </div>
            </div>

            <div className="col-lg-4">
              <div className="section-card mb-4 prediction-focus">
                <p className="text-uppercase text-xs text-secondary fw-bold mb-2">Slate</p>
                <h4 className="mb-3">{selectedDayLabel} ({dayLabelSuffix})</h4>
                <div className="row g-3">
                  <div className="col-6">
                    <div className="mlb-stat-tile">
                      <span>Predictions</span>
                      <strong>{activeMarketCount}</strong>
                    </div>
                  </div>
                  <div className="col-6">
                    <div className="mlb-stat-tile">
                      <span>Best Value</span>
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
                  <p className="text-xs text-secondary fw-bold text-uppercase mb-2">Best Value</p>
                  <h5 className="mb-1">{bestEv?.player_name ?? "-"}</h5>
                  <p className="text-sm text-secondary mb-0">
                    {bestEv ? `${formatAmerican(bestEv.american_odds)} | ${formatMoney(bestEv.ev_per_dollar)} per $1` : "Open Home Run Value"}
                  </p>
                </div>
                <div className="border-top pt-3 mt-3">
                  <p className="text-xs text-secondary fw-bold text-uppercase mb-2">Simulation</p>
                  <h5 className="mb-1">
                    {simulationRunState.data
                      ? `${simulationRunState.data.game.away_abbreviation} ${simulationRunState.data.summary.away_avg_score.toFixed(1)} / ${simulationRunState.data.game.home_abbreviation} ${simulationRunState.data.summary.home_avg_score.toFixed(1)}`
                      : selectedSimulationGame
                        ? `${selectedSimulationGame.away_abbreviation} @ ${selectedSimulationGame.home_abbreviation}`
                        : "-"}
                  </h5>
                  <p className="text-sm text-secondary mb-0">
                    {simulationRunState.data
                      ? `${simulationRunState.data.iterations} sims / ${formatPct(simulationRunState.data.summary.home_win_probability)} home`
                      : "Open Simulation"}
                  </p>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}
