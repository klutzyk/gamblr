import { useEffect, useRef, useState } from "react";
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
  type MlbSimulationGame,
  type MlbSimulationGamesResponse,
  type MlbSimulationPitchLogRow,
  type MlbSimulationRunResponse,
} from "./api";
import logo from "./assets/logo2.png";

type UserRegion = "au" | "us" | "uk";
type MainTab = "predictions" | "home_run_ev" | "simulation";
type MlbDay = "auto" | "today" | "tomorrow" | "yesterday";
type MlbSort = "value_desc" | "value_asc" | "lineup_asc" | "player_az";
type SimulationPlaybackMode = "full" | "highlights";
type SimulationPlaybackSpeed = 1 | 2 | 4 | 8;
type ApiState<T> = {
  data: T | null;
  loading: boolean;
  error: string | null;
};

type SimulationBatterLine = {
  key: string;
  name: string;
  team: string;
  pa: number;
  ab: number;
  h: number;
  hr: number;
  tb: number;
  rbi: number;
  bb: number;
  k: number;
};

type SimulationPitcherLine = {
  key: string;
  name: string;
  team: string;
  outs: number;
  pitches: number;
  h: number;
  hr: number;
  bb: number;
  k: number;
  r: number;
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

function parseScore(value: string | null | undefined) {
  const [away, home] = String(value ?? "0-0")
    .split("-")
    .map((part) => Number(part));
  return {
    away: Number.isFinite(away) ? away : 0,
    home: Number.isFinite(home) ? home : 0,
  };
}

function baseRunnerName(row: MlbSimulationPitchLogRow | null, base: "first" | "second" | "third") {
  return row?.base_runners_after?.[base]?.name ?? row?.base_runners_before?.[base]?.name ?? null;
}

function baseOccupied(row: MlbSimulationPitchLogRow | null, base: "first" | "second" | "third") {
  return Boolean(baseRunnerName(row, base));
}

function formatBaseRunners(row: MlbSimulationPitchLogRow | null) {
  if (!row) return "Bases empty";
  const runners = [
    ["1B", baseRunnerName(row, "first")],
    ["2B", baseRunnerName(row, "second")],
    ["3B", baseRunnerName(row, "third")],
  ].filter(([, name]) => Boolean(name));
  if (!runners.length) return "Bases empty";
  return runners.map(([base, name]) => `${base}: ${name}`).join(" / ");
}

function formatPitchCount(row: MlbSimulationPitchLogRow | null) {
  if (!row) return "-";
  const balls = typeof row.balls_before === "number" ? Math.min(row.balls_before, 3) : 0;
  const strikes = typeof row.strikes_before === "number" ? Math.min(row.strikes_before, 2) : 0;
  return `${balls}-${strikes}`;
}

function formatWindDirection(value: number | null | undefined) {
  if (typeof value !== "number" || Number.isNaN(value)) return "-";
  const directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"];
  const index = Math.round((((value % 360) + 360) % 360) / 45) % directions.length;
  return `${Math.round(value)} deg ${directions[index]}`;
}

function formatWindLine(row: MlbSimulationPitchLogRow | null) {
  if (!row || typeof row.wind_speed_mph !== "number") return "wind n/a";
  if (row.wind_speed_mph < 0.5) return "calm wind";
  const direction = formatWindDirection(row.wind_direction_deg);
  return direction === "-"
    ? `wind ${formatNumber(row.wind_speed_mph, 1)} mph`
    : `wind ${formatNumber(row.wind_speed_mph, 1)} mph ${direction}`;
}

function formatInningState(row: MlbSimulationPitchLogRow | null) {
  if (!row) return "Pre-game";
  const half = row.half.toLowerCase().startsWith("bot") ? "Bot" : "Top";
  return `${half} ${row.inning}`;
}

function formatOutState(value: number | null | undefined) {
  if (typeof value !== "number") return "0 out";
  return `${value} ${value === 1 ? "out" : "outs"}`;
}

function weatherSourceLabel(mode: string | null | undefined, count: number | null | undefined) {
  if (mode === "snapshots" && count && count > 0) return `${count} pitch-weather points`;
  if (mode === "snapshots") return "Pitch-weather loaded";
  return "Game forecast";
}

function isSimulationHighlight(row: MlbSimulationPitchLogRow) {
  return Boolean(row.result || (row.runs_scored ?? 0) > 0 || row.plate_appearance_result === "walk");
}

function normalizedPitchResult(row: MlbSimulationPitchLogRow) {
  return String(row.result ?? row.plate_appearance_result ?? row.call ?? "")
    .toLowerCase()
    .replace(/\s+/g, "_");
}

function totalBasesForResult(result: string) {
  if (result === "single") return 1;
  if (result === "double") return 2;
  if (result === "triple") return 3;
  if (result === "home_run") return 4;
  return 0;
}

function isPlateAppearanceResult(row: MlbSimulationPitchLogRow) {
  return Boolean(row.result || row.plate_appearance_result === "walk" || row.plate_appearance_result === "strikeout");
}

function formatInningsPitched(outs: number) {
  return `${Math.floor(outs / 3)}.${outs % 3}`;
}

function buildSimulationBoxScore(
  result: MlbSimulationRunResponse,
  activePitchNumber: number | null
) {
  const batters = new Map<string, SimulationBatterLine>();
  const pitchers = new Map<string, SimulationPitcherLine>();
  const rows = activePitchNumber
    ? result.sample.pitch_log.filter((row) => row.pitch_number <= activePitchNumber)
    : [];

  const getBatter = (row: MlbSimulationPitchLogRow) => {
    const team = row.half.toLowerCase().startsWith("top")
      ? result.game.away_abbreviation
      : result.game.home_abbreviation;
    const key = `${row.batter_id ?? row.batter}-${team}`;
    const existing = batters.get(key);
    if (existing) return existing;
    const next: SimulationBatterLine = {
      key,
      name: row.batter,
      team,
      pa: 0,
      ab: 0,
      h: 0,
      hr: 0,
      tb: 0,
      rbi: 0,
      bb: 0,
      k: 0,
    };
    batters.set(key, next);
    return next;
  };

  const getPitcher = (row: MlbSimulationPitchLogRow) => {
    const team = row.half.toLowerCase().startsWith("top")
      ? result.game.home_abbreviation
      : result.game.away_abbreviation;
    const key = `${row.pitcher_id ?? row.pitcher}-${team}`;
    const existing = pitchers.get(key);
    if (existing) return existing;
    const next: SimulationPitcherLine = {
      key,
      name: row.pitcher,
      team,
      outs: 0,
      pitches: 0,
      h: 0,
      hr: 0,
      bb: 0,
      k: 0,
      r: 0,
    };
    pitchers.set(key, next);
    return next;
  };

  rows.forEach((row) => {
    const batter = getBatter(row);
    const pitcher = getPitcher(row);
    const resultKey = normalizedPitchResult(row);
    const totalBases = totalBasesForResult(resultKey);
    const runs = row.runs_scored ?? 0;
    pitcher.pitches += 1;
    pitcher.outs += Math.max(0, (row.outs_after ?? row.outs_before) - row.outs_before);
    pitcher.r += runs;

    if (!isPlateAppearanceResult(row)) return;
    batter.pa += 1;
    batter.rbi += runs;
    if (resultKey === "walk") {
      batter.bb += 1;
      pitcher.bb += 1;
      return;
    }
    batter.ab += 1;
    if (resultKey === "strikeout") {
      batter.k += 1;
      pitcher.k += 1;
      return;
    }
    if (totalBases > 0) {
      batter.h += 1;
      batter.tb += totalBases;
      pitcher.h += 1;
      if (resultKey === "home_run") {
        batter.hr += 1;
        pitcher.hr += 1;
      }
    }
  });

  return {
    batters: Array.from(batters.values()),
    pitchers: Array.from(pitchers.values()),
  };
}

function SimulationField({
  result,
  activePitch,
  activeIndex,
  maxEventIndex,
  isPlaying,
  playbackSpeed,
  onEventIndexChange,
  onPlayingChange,
  onPlaybackSpeedChange,
}: {
  result: MlbSimulationRunResponse;
  activePitch: MlbSimulationPitchLogRow | null;
  activeIndex: number;
  maxEventIndex: number;
  isPlaying: boolean;
  playbackSpeed: SimulationPlaybackSpeed;
  onEventIndexChange: (index: number) => void;
  onPlayingChange: (playing: boolean) => void;
  onPlaybackSpeedChange: (speed: SimulationPlaybackSpeed) => void;
}) {
  const hasContact = typeof activePitch?.field_x === "number" && typeof activePitch.field_y === "number";
  const score = parseScore(activePitch?.score);
  const balls = activePitch ? Math.min(activePitch.balls_before, 3) : 0;
  const strikes = activePitch ? Math.min(activePitch.strikes_before, 2) : 0;
  const outs = activePitch?.outs_after ?? activePitch?.outs_before ?? 0;
  const contactX = activePitch?.field_x ?? 50;
  const contactY = activePitch?.field_y ?? 48;
  const hitEndX = Math.max(14, Math.min(146, 20 + contactX * 1.2));
  const hitEndY = Math.max(8, Math.min(62, 8 + contactY * 0.52));
  const pitchClass = hasContact ? "is-contact" : activePitch?.call === "ball" ? "is-ball" : "is-strike";
  const venueName =
    typeof result.game.venue?.name === "string"
      ? result.game.venue.name
      : typeof result.game.venue?.venue_name === "string"
        ? result.game.venue.venue_name
        : "Ballpark";
  const umpireName =
    typeof result.game.home_plate_umpire?.full_name === "string"
      ? result.game.home_plate_umpire.full_name
      : "Umpire TBD";
  const pitchOutcome = activePitch
    ? formatSimulationResult(activePitch.result ?? activePitch.plate_appearance_result ?? activePitch.call)
    : "Ready";
  return (
    <div key={activePitch?.pitch_number ?? "empty"} className={`simulation-field-wrap simulation-broadcast-field ${pitchClass}`}>
      <svg className="simulation-field" viewBox="0 0 160 90" role="img" aria-label="Simulated broadcast pitch view">
        <defs>
          <linearGradient id="broadcastSky" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor="#0b1717" />
            <stop offset="100%" stopColor="#12221c" />
          </linearGradient>
          <linearGradient id="broadcastGrass" x1="0" x2="1" y1="0" y2="1">
            <stop offset="0%" stopColor="#6fa33d" />
            <stop offset="48%" stopColor="#4b842f" />
            <stop offset="100%" stopColor="#2e641f" />
          </linearGradient>
          <linearGradient id="broadcastDirt" x1="0" x2="1" y1="0" y2="1">
            <stop offset="0%" stopColor="#a46b49" />
            <stop offset="100%" stopColor="#6f4330" />
          </linearGradient>
          <filter id="simGlow">
            <feGaussianBlur stdDeviation="1.4" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
        </defs>
        <rect width="160" height="90" fill="url(#broadcastSky)" />
        <g opacity="0.72">
          <rect x="0" y="2" width="160" height="28" fill="#12211e" />
          {Array.from({ length: 9 }).map((_, row) => (
            <path
              key={`seat-row-${row}`}
              d={`M0 ${7 + row * 2.8} H160`}
              stroke={row % 2 === 0 ? "rgba(248,241,221,0.08)" : "rgba(0,0,0,0.22)"}
              strokeWidth="1"
            />
          ))}
          {Array.from({ length: 18 }).map((_, index) => (
            <circle
              key={`seat-dot-${index}`}
              cx={8 + index * 8.7}
              cy={8 + (index % 5) * 4.4}
              r="0.7"
              fill="rgba(248,241,221,0.22)"
            />
          ))}
        </g>
        <rect x="0" y="27" width="160" height="11" fill="#1c2d2d" />
        <rect x="9" y="29" width="34" height="5" rx="1" fill="#215eb4" opacity="0.85" />
        <rect x="48" y="29" width="32" height="5" rx="1" fill="#b41f55" opacity="0.78" />
        <rect x="85" y="29" width="31" height="5" rx="1" fill="#1e7f68" opacity="0.82" />
        <rect x="121" y="29" width="29" height="5" rx="1" fill="#d1a529" opacity="0.75" />
        <path d="M0 90 L0 38 Q80 48 160 38 L160 90 Z" fill="url(#broadcastGrass)" />
        <path d="M38 90 L76 43 L84 43 L122 90 Z" fill="rgba(247,241,221,0.2)" />
        <path d="M42 90 L77 44 M118 90 L83 44" stroke="rgba(255,255,255,0.58)" strokeWidth="0.8" />
        <ellipse cx="80" cy="63.5" rx="25" ry="8.2" fill="url(#broadcastDirt)" />
        <ellipse cx="80" cy="61.5" rx="6.8" ry="2.4" fill="#d9b684" opacity="0.8" />
        <path d="M66 41 Q80 35 94 41 L89 48 Q80 45 71 48 Z" fill="url(#broadcastDirt)" />
        <path d="M77.5 43 L82.5 43 L84 46 L80 48 L76 46 Z" fill="#f8f1dd" opacity="0.86" />
        <path className="simulation-pitch-tunnel" d="M80 60 Q82 50 80 44" fill="none" stroke="rgba(255,255,255,0.28)" strokeWidth="0.65" strokeDasharray="2 2" />

        <g className="simulation-player simulation-catcher-model">
          <circle cx="80" cy="39.5" r="2.1" fill="#1e2630" />
          <path d="M76.8 42 Q80 46 83.2 42 L85 49 L75 49 Z" fill="#354251" />
          <path d="M76 49 L72 54 M84 49 L88 54" stroke="#354251" strokeWidth="2.1" strokeLinecap="round" />
        </g>
        <g className="simulation-player simulation-umpire-model">
          <circle cx="80" cy="35.7" r="1.8" fill="#111820" />
          <path d="M78.2 37.5 L77 44 L83 44 L81.8 37.5 Z" fill="#1b252c" />
        </g>
        <g className={`simulation-player simulation-batter-model ${hasContact ? "swing" : "track"}`}>
          <circle cx="103" cy="38" r="2.2" fill="#f3efe0" />
          <path d="M102 40 L99.8 48 L107 49 L105 40 Z" fill="#e8efe9" />
          <path d="M101 48 L98 56 M106 49 L110 56" stroke="#e8efe9" strokeWidth="2.1" strokeLinecap="round" />
          <path d="M102 42 L110 36" stroke="#e8efe9" strokeWidth="1.8" strokeLinecap="round" />
          <path className="simulation-bat" d="M109 35 L119 27" stroke="#d7a057" strokeWidth="1.5" strokeLinecap="round" />
        </g>
        <g className="simulation-player simulation-pitcher-model">
          <circle cx="80" cy="53.8" r="2.5" fill="#ecefee" />
          <path d="M79 56 L75.5 66 L84.5 66 L81 56 Z" fill="#dfe4e7" />
          <path className="simulation-pitch-arm" d="M80 57 L90 56" stroke="#dfe4e7" strokeWidth="2.1" strokeLinecap="round" />
          <path d="M79 65 L73 75 M83 65 L89 74" stroke="#dfe4e7" strokeWidth="2.3" strokeLinecap="round" />
          <path d="M76 58 L70 62" stroke="#dfe4e7" strokeWidth="2.1" strokeLinecap="round" />
        </g>

        {activePitch && (
          <>
            <path
              className="simulation-ball-path simulation-pitch-path"
              d="M87 56 Q83 49 80 43"
              fill="none"
              stroke={activePitch.call === "ball" ? "#78c8ff" : "#f8de55"}
              strokeWidth="1.35"
              strokeLinecap="round"
              filter="url(#simGlow)"
            />
            {hasContact ? (
              <path
                className="simulation-ball-path simulation-hit-path"
                d={`M86 42 Q${(86 + hitEndX) / 2} ${Math.min(18, hitEndY)} ${hitEndX} ${hitEndY}`}
                fill="none"
                stroke="#f6e55b"
                strokeWidth="1.55"
                strokeLinecap="round"
                filter="url(#simGlow)"
              />
            ) : null}
            {hasContact ? (
              <g className="simulation-hit-landing" transform={`translate(${hitEndX} ${hitEndY})`}>
                <circle r="3" fill="rgba(246,229,91,0.24)" stroke="#f6e55b" strokeWidth="0.75" />
                <text x="4.5" y="-3.2">
                  {normalizedPitchResult(activePitch).includes("out") ? "CAUGHT" : "LAND"}
                </text>
              </g>
            ) : null}
            <circle
              className="simulation-active-ball"
              cx={hasContact ? hitEndX : 80}
              cy={hasContact ? hitEndY : 43}
              r={hasContact ? 1.85 : 1.2}
              fill="#ffffff"
              stroke={hasContact ? "#f6e55b" : "#83c7ff"}
              strokeWidth="1"
              filter="url(#simGlow)"
            />
          </>
        )}
      </svg>

      <div className="simulation-venue-ribbon">
        <span>{venueName}</span>
        <strong>{formatNumber(activePitch?.temperature_f, 0)}F / {formatWindLine(activePitch)}</strong>
        <small>{weatherSourceLabel(result.inputs.weather_mode, result.game.weather_snapshot_count)} / {umpireName}</small>
      </div>

      <div className="simulation-player-tag simulation-player-tag-pitcher">
        <span>Pitcher</span>
        <strong>{activePitch?.pitcher ?? "-"}</strong>
      </div>
      <div className="simulation-player-tag simulation-player-tag-batter">
        <span>Batter</span>
        <strong>{activePitch?.batter ?? "-"}</strong>
      </div>

      <div className="simulation-metrics-rail" aria-label="Pitch and contact metrics">
        <div>
          <span>Pitch</span>
          <strong>{activePitch ? `${activePitch.pitch_type} ${formatNumber(activePitch.pitch_mph, 1)}` : "-"}</strong>
          <small>{activePitch?.pitch_description ?? "mph"}</small>
        </div>
        <div>
          <span>Movement</span>
          <strong>
            H {formatNumber(activePitch?.pitch_break_horizontal, 1)} / V {formatNumber(activePitch?.pitch_break_vertical, 1)}
          </strong>
          <small>{formatNumber(activePitch?.pitch_spin_rate, 0)} rpm</small>
        </div>
        <div>
          <span>Contact</span>
          <strong>{typeof activePitch?.launch_speed === "number" ? `${formatNumber(activePitch.launch_speed, 1)} mph` : "No BIP"}</strong>
          <small>
            {typeof activePitch?.launch_angle === "number" || typeof activePitch?.distance_ft === "number"
              ? `${formatNumber(activePitch?.launch_angle, 1)} deg / ${formatNumber(activePitch?.distance_ft, 0)} ft`
              : "awaiting ball in play"}
          </small>
        </div>
        <div>
          <span>Carry</span>
          <strong>{typeof activePitch?.wind_out_mph === "number" ? `${formatNumber(activePitch.wind_out_mph, 1)} mph` : "n/a"}</strong>
          <small>{typeof activePitch?.fence_ft === "number" ? `Fence ${formatNumber(activePitch.fence_ft, 0)} ft` : "contact only"}</small>
        </div>
      </div>

      <div className="simulation-broadcast-scorebug" aria-label="Live boxscore">
        <div className="simulation-score-lines">
          <div>
            <span>{result.game.away_abbreviation}</span>
            <strong>{score.away}</strong>
          </div>
          <div>
            <span>{result.game.home_abbreviation}</span>
            <strong>{score.home}</strong>
          </div>
        </div>
        <div className="simulation-game-state">
          <strong>{formatInningState(activePitch)}</strong>
          <span>{formatOutState(outs)}</span>
          <div className="simulation-count-dots" aria-label={`Count ${formatPitchCount(activePitch)}`}>
            <span className={balls >= 1 ? "on" : ""} />
            <span className={balls >= 2 ? "on" : ""} />
            <span className={balls >= 3 ? "on" : ""} />
            <i className={strikes >= 1 ? "on" : ""} />
            <i className={strikes >= 2 ? "on" : ""} />
          </div>
        </div>
        <div className="simulation-basebug" aria-label={formatBaseRunners(activePitch)}>
          <span
            className={`base second ${baseOccupied(activePitch, "second") ? "occupied" : ""}`}
            title={baseRunnerName(activePitch, "second") ?? "Second base empty"}
          />
          <span
            className={`base third ${baseOccupied(activePitch, "third") ? "occupied" : ""}`}
            title={baseRunnerName(activePitch, "third") ?? "Third base empty"}
          />
          <span
            className={`base first ${baseOccupied(activePitch, "first") ? "occupied" : ""}`}
            title={baseRunnerName(activePitch, "first") ?? "First base empty"}
          />
        </div>
      </div>

      <div className="simulation-field-playback" aria-label="Simulation playback controls">
        <button
          type="button"
          onClick={() => onEventIndexChange(Math.max(0, activeIndex - 1))}
          aria-label="Previous pitch"
        >
          <i className="material-symbols-rounded">chevron_left</i>
        </button>
        <button
          type="button"
          className="simulation-play-toggle"
          onClick={() => onPlayingChange(!isPlaying)}
          aria-label={isPlaying ? "Pause simulation" : "Play simulation"}
        >
          <i className="material-symbols-rounded">{isPlaying ? "pause" : "play_arrow"}</i>
        </button>
        <button
          type="button"
          onClick={() => onEventIndexChange(Math.min(maxEventIndex, activeIndex + 1))}
          aria-label="Next pitch"
        >
          <i className="material-symbols-rounded">chevron_right</i>
        </button>
        <div className="simulation-speed-set" aria-label="Playback speed">
          {([1, 2, 4, 8] as SimulationPlaybackSpeed[]).map((speed) => (
            <button
              key={speed}
              type="button"
              className={playbackSpeed === speed ? "active" : ""}
              onClick={() => onPlaybackSpeedChange(speed)}
            >
              {speed}x
            </button>
          ))}
        </div>
        <input
          type="range"
          min={0}
          max={maxEventIndex}
          value={activeIndex}
          onChange={(event) => onEventIndexChange(Number(event.target.value))}
          aria-label="Pitch timeline"
        />
      </div>

      <div className="simulation-lower-third">
        <div>
          <span>{activePitch ? `Pitch ${activePitch.pitch_number} / ${formatPitchCount(activePitch)}` : "Pitch simulation"}</span>
          <strong>{pitchOutcome}</strong>
        </div>
        <div>
          <span>Runners</span>
          <strong>{formatBaseRunners(activePitch)}</strong>
        </div>
      </div>
    </div>
  );
}

function SimulationResults({
  result,
  activeEventIndex,
  onEventIndexChange,
  playbackMode,
  onPlaybackModeChange,
  isPlaying,
  onPlayingChange,
  playbackSpeed,
  onPlaybackSpeedChange,
}: {
  result: MlbSimulationRunResponse;
  activeEventIndex: number;
  onEventIndexChange: (index: number) => void;
  playbackMode: SimulationPlaybackMode;
  onPlaybackModeChange: (mode: SimulationPlaybackMode) => void;
  isPlaying: boolean;
  onPlayingChange: (playing: boolean) => void;
  playbackSpeed: SimulationPlaybackSpeed;
  onPlaybackSpeedChange: (speed: SimulationPlaybackSpeed) => void;
}) {
  const timeline =
    playbackMode === "full"
      ? result.sample.pitch_log
      : result.sample.pitch_log.filter(isSimulationHighlight);
  const maxEventIndex = Math.max(timeline.length - 1, 0);
  const activeIndex = Math.min(activeEventIndex, maxEventIndex);
  const activePitch = timeline[activeIndex] ?? null;
  const pitchRows = result.sample.pitch_log;
  const liveBoxScore = buildSimulationBoxScore(result, activePitch?.pitch_number ?? null);

  useEffect(() => {
    if (!isPlaying || timeline.length <= 1) return;
    const delay = Math.max(120, Math.round(900 / playbackSpeed));
    const timer = window.setInterval(() => {
      onEventIndexChange(Math.min(maxEventIndex, activeIndex + 1));
      if (activeIndex >= maxEventIndex) {
        onPlayingChange(false);
      }
    }, delay);
    return () => window.clearInterval(timer);
  }, [activeIndex, isPlaying, maxEventIndex, onEventIndexChange, onPlayingChange, playbackSpeed, timeline.length]);

  return (
    <div className="simulation-results mt-3">
      <div className="simulation-playback-bar">
        <div className="stat-toggle simulation-mode-toggle" role="group" aria-label="Simulation playback mode">
          <button
            className={`stat-chip ${playbackMode === "full" ? "active" : ""}`}
            type="button"
            onClick={() => {
              onPlaybackModeChange("full");
              onEventIndexChange(0);
              onPlayingChange(false);
            }}
          >
            Full
          </button>
          <button
            className={`stat-chip ${playbackMode === "highlights" ? "active" : ""}`}
            type="button"
            onClick={() => {
              onPlaybackModeChange("highlights");
              onEventIndexChange(0);
              onPlayingChange(false);
            }}
          >
            Highlights
          </button>
        </div>
        <div className="simulation-playback-copy">
          <strong>{playbackMode === "full" ? "Every pitch" : "Contact, walks, scoring"}</strong>
          <span>{timeline.length} timeline events</span>
        </div>
      </div>

      <div className="simulation-stage-full mt-3">
        <SimulationField
          result={result}
          activePitch={activePitch}
          activeIndex={activeIndex}
          maxEventIndex={maxEventIndex}
          isPlaying={isPlaying}
          playbackSpeed={playbackSpeed}
          onEventIndexChange={(index) => {
            onEventIndexChange(index);
            if (index >= maxEventIndex) onPlayingChange(false);
          }}
          onPlayingChange={onPlayingChange}
          onPlaybackSpeedChange={onPlaybackSpeedChange}
        />
      </div>

      <div className="simulation-live-boxscores mt-3">
        <div className="simulation-boxscore-panel">
          <div className="simulation-boxscore-title">
            <span>Live batting boxscore</span>
            <strong>{activePitch ? `Through pitch ${activePitch.pitch_number}` : "Pre-game"}</strong>
          </div>
          <div className="table-responsive simulation-table-wrap simulation-boxscore-table">
            <table className="table align-items-center mb-0 mlb-ev-table">
              <thead>
                <tr>
                  <th>Team</th>
                  <th>Batter</th>
                  <th className="text-center">AB</th>
                  <th className="text-center">H</th>
                  <th className="text-center">HR</th>
                  <th className="text-center">TB</th>
                  <th className="text-center">RBI</th>
                  <th className="text-center">BB</th>
                  <th className="text-center">K</th>
                </tr>
              </thead>
              <tbody>
                {liveBoxScore.batters.map((row) => (
                  <tr key={row.key} className={row.name === activePitch?.batter ? "simulation-active-row" : ""}>
                    <td className="fw-bold">{row.team}</td>
                    <td className="fw-bold">{row.name}</td>
                    <td className="text-center">{row.ab}</td>
                    <td className="text-center">{row.h}</td>
                    <td className="text-center">{row.hr}</td>
                    <td className="text-center">{row.tb}</td>
                    <td className="text-center">{row.rbi}</td>
                    <td className="text-center">{row.bb}</td>
                    <td className="text-center">{row.k}</td>
                  </tr>
                ))}
                {liveBoxScore.batters.length === 0 && (
                  <tr>
                    <td colSpan={9} className="text-center text-secondary py-3">
                      Advance the simulation to populate the live boxscore.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        <div className="simulation-boxscore-panel">
          <div className="simulation-boxscore-title">
            <span>Live pitching line</span>
            <strong>{result.game.away_abbreviation} @ {result.game.home_abbreviation}</strong>
          </div>
          <div className="table-responsive simulation-table-wrap simulation-boxscore-table">
            <table className="table align-items-center mb-0 mlb-ev-table">
              <thead>
                <tr>
                  <th>Team</th>
                  <th>Pitcher</th>
                  <th className="text-center">IP</th>
                  <th className="text-center">P</th>
                  <th className="text-center">K</th>
                  <th className="text-center">H</th>
                  <th className="text-center">HR</th>
                  <th className="text-center">BB</th>
                  <th className="text-center">R</th>
                </tr>
              </thead>
              <tbody>
                {liveBoxScore.pitchers.map((row) => (
                  <tr key={row.key} className={row.name === activePitch?.pitcher ? "simulation-active-row" : ""}>
                    <td className="fw-bold">{row.team}</td>
                    <td className="fw-bold">{row.name}</td>
                    <td className="text-center">{formatInningsPitched(row.outs)}</td>
                    <td className="text-center">{row.pitches}</td>
                    <td className="text-center">{row.k}</td>
                    <td className="text-center">{row.h}</td>
                    <td className="text-center">{row.hr}</td>
                    <td className="text-center">{row.bb}</td>
                    <td className="text-center">{row.r}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className="simulation-meta-grid simulation-meta-grid-compact mt-3">
        <div>
          <span>{result.game.away_abbreviation} starter</span>
          <strong>{result.inputs.away_starter ?? "-"}</strong>
        </div>
        <div>
          <span>{result.game.home_abbreviation} starter</span>
          <strong>{result.inputs.home_starter ?? "-"}</strong>
        </div>
        <div>
          <span>Weather source</span>
          <strong>{weatherSourceLabel(result.inputs.weather_mode, result.game.weather_snapshot_count)}</strong>
        </div>
        <div>
          <span>Umpire</span>
          <strong>{String(result.game.home_plate_umpire?.full_name ?? "-")}</strong>
        </div>
      </div>

      <div className="row g-4 mt-1">
        <div className="col-xl-5">
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
        <div className="col-xl-7">
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
        <div className="col-12">
          <div className="table-responsive simulation-table-wrap simulation-pitch-log">
            <table className="table align-items-center mb-0 mlb-ev-table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Inning</th>
                  <th>Count</th>
                  <th>Batter</th>
                  <th>Pitcher</th>
                  <th>Pitch</th>
                  <th>Call</th>
                  <th>Runners</th>
                  <th>Weather</th>
                  <th>Score</th>
                </tr>
              </thead>
              <tbody>
                {pitchRows.map((row) => (
                  <tr
                    key={row.pitch_number}
                    className={row.pitch_number === activePitch?.pitch_number ? "simulation-active-row" : ""}
                    onClick={() => {
                      const nextIndex = timeline.findIndex((item) => item.pitch_number === row.pitch_number);
                      if (nextIndex >= 0) onEventIndexChange(nextIndex);
                    }}
                  >
                    <td>{row.pitch_number}</td>
                    <td>
                      {row.half} {row.inning}
                    </td>
                    <td>{formatPitchCount(row)}</td>
                    <td className="fw-bold">{row.batter}</td>
                    <td>{row.pitcher}</td>
                    <td>
                      {row.pitch_type} {formatNumber(row.pitch_mph, 1)}
                    </td>
                    <td>{formatSimulationResult(row.result ?? row.call)}</td>
                    <td>{formatBaseRunners(row)}</td>
                    <td>
                      {formatNumber(row.temperature_f, 0)}F / {formatNumber(row.wind_speed_mph, 1)} mph
                    </td>
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
  const [simulationPlaybackMode, setSimulationPlaybackMode] = useState<SimulationPlaybackMode>("full");
  const [simulationPlaying, setSimulationPlaying] = useState(false);
  const [simulationPlaybackSpeed, setSimulationPlaybackSpeed] = useState<SimulationPlaybackSpeed>(1);
  const simulationResultsRef = useRef<HTMLDivElement | null>(null);
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
    setSimulationPlaying(false);
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
    setSimulationPlaying(false);
    try {
      const parsedSeed = simulationSeed.trim() ? Number(simulationSeed.trim()) : undefined;
      const data = await runMlbGameSimulation({
        game_pk: selectedSimulationGamePk,
        iterations: simulationIterations,
        seed: Number.isFinite(parsedSeed) ? parsedSeed : undefined,
        pitch_log_limit: 1400,
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

  useEffect(() => {
    if (mainTab !== "simulation" || !simulationRunState.data || simulationRunState.loading) return;
    window.requestAnimationFrame(() => {
      const stage = simulationResultsRef.current?.querySelector<HTMLElement>(".simulation-stage-full");
      (stage ?? simulationResultsRef.current)?.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  }, [mainTab, simulationRunState.data, simulationRunState.loading]);

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
            <div className={mainTab === "simulation" ? "col-lg-12" : "col-lg-8"}>
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
                              setSimulationPlaying(false);
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
                    {selectedSimulationGame && !simulationRunState.data && (
                      <div className="simulation-game-strip mt-4">
                        <div>
                          <span>Venue</span>
                          <strong>{selectedSimulationGame.venue_name ?? "-"}</strong>
                        </div>
                        <div>
                          <span>Weather source</span>
                          <strong>
                            {selectedSimulationGame.weather_available
                              ? `${selectedSimulationGame.weather_rows ?? 0} pitch-weather points`
                              : "Game forecast"}
                          </strong>
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
                      <div ref={simulationResultsRef}>
                      <SimulationResults
                        result={simulationRunState.data}
                        activeEventIndex={simulationEventIndex}
                        onEventIndexChange={setSimulationEventIndex}
                        playbackMode={simulationPlaybackMode}
                        onPlaybackModeChange={setSimulationPlaybackMode}
                        isPlaying={simulationPlaying}
                        onPlayingChange={setSimulationPlaying}
                        playbackSpeed={simulationPlaybackSpeed}
                        onPlaybackSpeedChange={setSimulationPlaybackSpeed}
                      />
                      </div>
                    )}
                  </>
                )}
              </div>
            </div>

            {mainTab !== "simulation" && (
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
            )}
          </div>
        </div>
      </section>
    </div>
  );
}
