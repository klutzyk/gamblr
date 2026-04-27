import { useEffect, useMemo, useState, type Dispatch, type SetStateAction } from "react";
import "./App.css";
import "./PerformancePage.css";
import logo from "./assets/logo2.png";
import {
  getReviewOverview,
  getReviewPlayers,
  getReviewRecent,
  getReviewTrend,
  getReviewPlayerDetail,
  type ReviewOverview,
  type ReviewPlayersResponse,
  type ReviewRecentResponse,
  type ReviewTrendResponse,
  type ReviewPlayerDetail,
} from "./api";

type UserRegion = "au" | "us" | "uk";
type StatType = "points" | "assists" | "rebounds" | "threept" | "threepa";
type PlayerSortKey = "full_name" | "average_miss" | "close_rate" | "trend_label";
type SortDirection = "asc" | "desc";

type ApiState<T> = {
  loading: boolean;
  error: string | null;
  data: T | null;
};

const initialState = <T,>(): ApiState<T> => ({
  loading: false,
  error: null,
  data: null,
});

const REGION_CONFIG: Record<
  UserRegion,
  { label: string; locale: string; timeZone: string; short: string }
> = {
  au: {
    label: "Australia",
    locale: "en-AU",
    timeZone: "Australia/Sydney",
    short: "AEST",
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

const STAT_OPTIONS: Array<{ value: StatType; label: string; unit: string }> = [
  { value: "points", label: "Points", unit: "pts" },
  { value: "assists", label: "Assists", unit: "ast" },
  { value: "rebounds", label: "Rebounds", unit: "reb" },
  { value: "threept", label: "3PT Made", unit: "3PM" },
  { value: "threepa", label: "3PT Attempts", unit: "3PA" },
];

const DAY_OPTIONS = [7, 14, 30, 60, 90];
const PLAYER_PAGE_SIZE = 12;
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

function formatNumber(value: number | null | undefined, digits = 1) {
  return typeof value === "number" && Number.isFinite(value) ? value.toFixed(digits) : "-";
}

function getInitials(name: string | null | undefined) {
  return (name ?? "")
    .split(" ")
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => part[0])
    .join("")
    .toUpperCase() || "NA";
}

function getHeadshotUrl(playerId: number, teamAbbr?: string | null) {
  const teamId = teamAbbr ? TEAM_ID_BY_ABBR[teamAbbr] : undefined;
  if (!teamId) return "";
  return `https://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/${teamId}/2025/260x190/${playerId}.png`;
}

function formatSignedNumber(value: number | null | undefined, digits = 1) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "-";
  return `${value >= 0 ? "+" : "-"}${Math.abs(value).toFixed(digits)}`;
}

function getSignedDeltaToneClass(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "perf-delta-neutral";
  if (value > 0) return "perf-delta-positive";
  if (value < 0) return "perf-delta-negative";
  return "perf-delta-neutral";
}

function formatPercent(value: number | null | undefined) {
  return typeof value === "number" && Number.isFinite(value) ? `${Math.round(value * 100)}%` : "-";
}

function formatDateByRegion(
  dateString: string | null | undefined,
  regionConfig: (typeof REGION_CONFIG)[UserRegion]
) {
  if (!dateString) return "-";
  const parsed = new Date(`${dateString}T12:00:00`);
  if (Number.isNaN(parsed.getTime())) return dateString;
  return new Intl.DateTimeFormat(regionConfig.locale, {
    timeZone: regionConfig.timeZone,
    day: "numeric",
    month: "short",
  }).format(parsed);
}

function getShiftedGameDate(dateString: string | null | undefined, userRegion: UserRegion) {
  if (!dateString) return null;
  const parts = dateString.split("-").map(Number);
  if (parts.length !== 3 || parts.some((part) => Number.isNaN(part))) return null;
  const [year, month, day] = parts;
  const shiftDays = userRegion === "us" ? 0 : 1;
  return new Date(Date.UTC(year, month - 1, day + shiftDays));
}

function formatGameDateByRegion(
  dateString: string | null | undefined,
  regionConfig: (typeof REGION_CONFIG)[UserRegion],
  userRegion: UserRegion
) {
  const shifted = getShiftedGameDate(dateString, userRegion);
  if (!shifted) return dateString ?? "-";
  return new Intl.DateTimeFormat(regionConfig.locale, {
    timeZone: "UTC",
    day: "numeric",
    month: "short",
  }).format(shifted);
}

function formatGameDateShortByRegion(
  dateString: string | null | undefined,
  userRegion: UserRegion
) {
  const shifted = getShiftedGameDate(dateString, userRegion);
  if (!shifted) return dateString ? dateString.slice(5).replace("-", "/") : "-";
  const month = String(shifted.getUTCMonth() + 1).padStart(2, "0");
  const day = String(shifted.getUTCDate()).padStart(2, "0");
  return `${month}/${day}`;
}

function getBiasDirectionLabel(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "Balanced";
  if (value >= 0.35) return "Above actual";
  if (value <= -0.35) return "Below actual";
  return "Balanced";
}

function getBiasDirectionToneClass(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "tone-neutral";
  if (value >= 0.35) return "tone-warm";
  if (value <= -0.35) return "tone-good";
  return "tone-neutral";
}

function getBiasValueToneClass(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "perf-value-neutral";
  if (value > 0) return "perf-value-low";
  if (value < 0) return "perf-value-high";
  return "perf-value-neutral";
}

function getBiasDirectionExplanation(value: number | null | undefined, unit: string) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "Not enough completed results yet.";
  }
  if (value >= 0.35) {
    return `Predictions have been finishing about ${Math.abs(value).toFixed(1)} ${unit} above the real result on average.`;
  }
  if (value <= -0.35) {
    return `Predictions have been finishing about ${Math.abs(value).toFixed(1)} ${unit} below the real result on average.`;
  }
  return `Predictions have been staying close to balanced versus the real result.`;
}

function renderPlayerDirectionCopy(
  detail: ReviewPlayerDetail,
  unit: string
) {
  const tracked = detail.tracked_predictions;
  const miss = formatNumber(detail.average_miss);
  const bias = detail.bias;

  if (typeof bias !== "number" || !Number.isFinite(bias)) {
    return (
      <>
        {detail.full_name} has {tracked} tracked predictions in this view. The average miss is{" "}
        <span className="perf-player-story-miss">{miss} {unit}</span>.
      </>
    );
  }

  if (Math.abs(bias) < 0.35) {
    return (
      <>
        {detail.full_name} has {tracked} tracked predictions in this view. The average miss is{" "}
        <span className="perf-player-story-miss">{miss} {unit}</span>, and the model has been running{" "}
        <span className="perf-player-story-delta">
          {formatSignedNumber(bias)} {unit}
        </span>{" "}
        from the real result on average.
      </>
    );
  }

  return (
    <>
      {detail.full_name} has {tracked} tracked predictions in this view. The average miss is{" "}
      <span className="perf-player-story-miss">{miss} {unit}</span>, and the model has been running{" "}
      <span className="perf-player-story-delta">
        {formatSignedNumber(bias)} {unit}
      </span>{" "}
      which means it has been predicting {bias > 0 ? "higher than his actual" : "lower than his actual"} on average.
    </>
  );
}

function isDnpGame(game: ReviewPlayerDetail["games"][number]) {
  return (
    typeof game.actual === "number" &&
    game.actual === 0 &&
    typeof game.minutes === "number" &&
    game.minutes <= 0.5
  );
}

function getMissToneClass(value: number | null | undefined, statType: StatType) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "tone-neutral";
  const thresholds: Record<StatType, { good: number; mixed: number }> = {
    points: { good: 4.0, mixed: 6.0 },
    assists: { good: 1.8, mixed: 2.7 },
    rebounds: { good: 2.0, mixed: 3.0 },
    threept: { good: 0.9, mixed: 1.3 },
    threepa: { good: 1.8, mixed: 2.8 },
  };
  const { good, mixed } = thresholds[statType];
  if (value <= good) return "tone-good";
  if (value <= mixed) return "tone-neutral";
  return "tone-warm";
}

function getMissReadLabel(value: number | null | undefined, statType: StatType) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "Still building";
  const thresholds: Record<StatType, { good: number; mixed: number }> = {
    points: { good: 4.0, mixed: 6.0 },
    assists: { good: 1.8, mixed: 2.7 },
    rebounds: { good: 2.0, mixed: 3.0 },
    threept: { good: 0.9, mixed: 1.3 },
    threepa: { good: 1.8, mixed: 2.8 },
  };
  const { good, mixed } = thresholds[statType];
  if (value <= good) return "Good";
  if (value <= mixed) return "Mixed";
  return "Needs work";
}

function getResultTone(
  miss: number | null | undefined,
  threshold: number | null | undefined
) {
  if (typeof miss !== "number" || !Number.isFinite(miss)) return "neutral";
  const safeThreshold = typeof threshold === "number" && Number.isFinite(threshold) ? threshold : 1;
  if (miss <= safeThreshold) return "good";
  if (miss <= safeThreshold * 1.5) return "neutral";
  return "high";
}

function getActualFillColor(
  predicted: number | null | undefined,
  actual: number | null | undefined,
  threshold: number | null | undefined
) {
  const pred = typeof predicted === "number" && Number.isFinite(predicted) ? predicted : 0;
  const act = typeof actual === "number" && Number.isFinite(actual) ? actual : 0;
  const safeThreshold = typeof threshold === "number" && Number.isFinite(threshold) ? threshold : 1;
  const miss = Math.abs(pred - act);
  const severity = Math.max(0, Math.min(1, miss / Math.max(safeThreshold * 1.8, 1)));

  if (act <= pred) {
    const lightness = 78 - severity * 14;
    const saturation = 62 + severity * 14;
    return `hsl(4 ${saturation}% ${lightness}%)`;
  }

  const lightness = 78 - severity * 14;
  const saturation = 48 + severity * 16;
  return `hsl(138 ${saturation}% ${lightness}%)`;
}

function getPlayerRowToneClass(value: number | null | undefined, statType: StatType) {
  return `player-${getMissToneClass(value, statType).replace("tone-", "")}`;
}

function getCloseCallExplanation(
  threshold: number | null | undefined,
  unit: string,
  statLabel: string
) {
  if (typeof threshold !== "number" || !Number.isFinite(threshold)) {
    return "Close calls are the predictions that landed near the real result.";
  }
  return `A close call for ${statLabel.toLowerCase()} means finishing within ${threshold.toFixed(1)} ${unit} of the real result.`;
}

function getPerformanceHeadline(overview: ReviewOverview | null) {
  if (!overview) return "We're still gathering enough completed results to judge the predictions properly.";
  return overview.story ?? "Here's a simple look at how our predictions have been performing lately.";
}

function safeLoad<T>(
  setState: Dispatch<SetStateAction<ApiState<T>>>,
  loader: () => Promise<T>
) {
  setState((prev) => ({ ...prev, loading: true, error: null }));
  return loader()
    .then((data) => {
      setState({ loading: false, error: null, data });
      return data;
    })
    .catch((error) => {
      const message = error instanceof Error ? error.message : "Failed to load.";
      setState({ loading: false, error: message, data: null });
      throw error;
    });
}

function TrendChart({
  points,
  regionConfig,
  userRegion,
  statType,
}: {
  points: ReviewTrendResponse["points"];
  regionConfig: (typeof REGION_CONFIG)[UserRegion];
  userRegion: UserRegion;
  statType: StatType;
}) {
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null);
  const visible = points.slice(-14);
  const numericMisses = visible
    .map((point) => point.average_miss)
    .filter((value): value is number => typeof value === "number" && Number.isFinite(value));

  if (visible.length === 0 || numericMisses.length === 0) {
    return <div className="perf-empty-chart">No completed days in this window yet.</div>;
  }

  const threshold = {
    points: 4,
    assists: 2,
    rebounds: 2,
    threept: 1,
    threepa: 2,
  }[statType];
  const maxMiss = Math.max(...numericMisses, threshold + 1);
  const chartHeight = 220;
  const chartWidth = 640;
  const leftPad = 42;
  const rightPad = 12;
  const topPad = 12;
  const bottomPad = 38;
  const innerHeight = chartHeight - topPad - bottomPad;
  const innerWidth = chartWidth - leftPad - rightPad;
  const stepX = innerWidth / Math.max(visible.length, 1);
  const yTicks = [0, maxMiss / 2, maxMiss].map((tick) => Number(tick.toFixed(1)));
  const hoveredPoint = hoveredIndex !== null ? visible[hoveredIndex] ?? null : null;

  const getBarTone = (miss: number) => {
    if (miss <= threshold) return "var(--perf-accent-deep)";
    if (miss <= threshold * 1.5) return "#d8b15b";
    return "var(--perf-danger)";
  };

  return (
    <div className="perf-trend-chart-wrap">
      {hoveredPoint ? (
        <div className="perf-hover-tooltip perf-hover-tooltip-trend" role="status" aria-live="polite">
          <strong>{formatGameDateByRegion(hoveredPoint.game_date, regionConfig, userRegion)}</strong>
          <span>Average miss {formatNumber(hoveredPoint.average_miss)}</span>
        </div>
      ) : null}
      <svg viewBox={`0 0 ${chartWidth} ${chartHeight}`} className="perf-trend-chart" preserveAspectRatio="none">
        {yTicks.map((tick) => {
          const y = topPad + innerHeight - (tick / maxMiss) * innerHeight;
          return (
            <g key={tick}>
              <line x1={leftPad} x2={chartWidth - rightPad} y1={y} y2={y} className="perf-trend-grid" />
              <text x={leftPad - 8} y={y + 4} textAnchor="end" className="perf-trend-axis-label">
                {tick.toFixed(1)}
              </text>
            </g>
          );
        })}
        {visible.map((point, index) => {
          const miss = point.average_miss ?? 0;
          const barHeight = (miss / maxMiss) * innerHeight;
          const x = leftPad + index * stepX + stepX * 0.16;
          const width = stepX * 0.68;
          const y = topPad + innerHeight - barHeight;
          const showLabel = visible.length <= 7 || index % 3 === 0 || index === visible.length - 1;
          return (
            <g key={point.game_date}>
              <rect
                className="perf-trend-bar-rect"
                x={x}
                y={y}
                width={width}
                height={barHeight}
                rx={Math.min(10, width / 3)}
                fill={getBarTone(miss)}
                onMouseEnter={() => setHoveredIndex(index)}
                onMouseLeave={() => setHoveredIndex((current) => (current === index ? null : current))}
              />
              {showLabel ? (
                <text
                  x={x + width / 2}
                  y={chartHeight - 14}
                  textAnchor="middle"
                  className="perf-trend-x-label"
                >
                  {formatGameDateByRegion(point.game_date, regionConfig, userRegion)}
                </text>
              ) : null}
            </g>
          );
        })}
      </svg>
    </div>
  );
}

function LineComparisonChart({
  games,
  unit,
  userRegion,
}: {
  games: ReviewPlayerDetail["games"];
  unit: string;
  userRegion: UserRegion;
}) {
  const [hoveredPoint, setHoveredPoint] = useState<{ index: number; x: number; y: number } | null>(null);
  const rows = [...games].reverse().filter(
    (game) =>
      typeof game.predicted === "number" &&
      Number.isFinite(game.predicted) &&
      typeof game.actual === "number" &&
      Number.isFinite(game.actual)
  );

  if (rows.length < 2) {
    return <div className="perf-empty-chart">Not enough completed games yet.</div>;
  }

  const allValues = rows.flatMap((game) => [game.predicted as number, game.actual as number]);
  const min = Math.min(...allValues);
  const max = Math.max(...allValues);
  const range = Math.max(max - min, 0.001);
  const width = 520;
  const height = 170;
  const left = 34;
  const right = 10;
  const top = 8;
  const bottom = 18;
  const innerWidth = width - left - right;
  const innerHeight = height - top - bottom;

  const toPoint = (value: number, index: number) => {
    const x = left + (index / Math.max(rows.length - 1, 1)) * innerWidth;
    const y = top + innerHeight - ((value - min) / range) * innerHeight;
    return { x, y };
  };

  const predictedPoints = rows.map((game, index) => toPoint(game.predicted as number, index));
  const actualPoints = rows.map((game, index) => toPoint(game.actual as number, index));
  const line = (pts: Array<{ x: number; y: number }>) => pts.map((pt) => `${pt.x},${pt.y}`).join(" ");
  const yTicks = [min, (min + max) / 2, max];
  const labelEvery = rows.length <= 6 ? 1 : 2;
  const hoveredGame = hoveredPoint ? rows[hoveredPoint.index] ?? null : null;
  const tooltipStyle = hoveredPoint
    ? {
        left: `calc(${(hoveredPoint.x / width) * 100}% + 14px)`,
        top: `calc(${(hoveredPoint.y / height) * 100}% - 10px)`,
      }
    : undefined;

  const setHoveredChartPoint = (index: number, x: number, y: number) => {
    setHoveredPoint({ index, x, y });
  };

  return (
    <div className="perf-line-chart-wrap">
      {hoveredGame ? (
        <div
          className="perf-hover-tooltip perf-hover-tooltip-line"
          style={tooltipStyle}
          role="status"
          aria-live="polite"
        >
          <strong>{formatGameDateShortByRegion(hoveredGame.game_date, userRegion)}</strong>
          <span>
            Pred {formatNumber(hoveredGame.predicted)} {unit}
          </span>
          <span>
            Actual {formatNumber(hoveredGame.actual)} {unit}
          </span>
          <span>
            Min{" "}
            {typeof hoveredGame.minutes === "number" && Number.isFinite(hoveredGame.minutes)
              ? Math.round(hoveredGame.minutes)
              : "-"}
          </span>
          <span>
            Miss {formatNumber(hoveredGame.average_miss)} {unit}
          </span>
        </div>
      ) : null}
      <svg
        viewBox={`0 0 ${width} ${height}`}
        className="perf-line-chart"
        preserveAspectRatio="none"
        onMouseLeave={() => setHoveredPoint(null)}
      >
        {yTicks.map((tick) => {
          const y = top + innerHeight - ((tick - min) / range) * innerHeight;
          return (
            <g key={tick}>
              <line x1={left} x2={width - right} y1={y} y2={y} className="perf-trend-grid" />
              <text x={left - 6} y={y + 4} textAnchor="end" className="perf-trend-axis-label">
                {formatNumber(tick)}
              </text>
            </g>
          );
        })}
        <polyline
          fill="none"
          stroke="#8b97a1"
          strokeWidth="2.25"
          strokeLinejoin="round"
          strokeLinecap="round"
          points={line(predictedPoints)}
        />
        <polyline
          fill="none"
          stroke="#63c768"
          strokeWidth="2.25"
          strokeLinejoin="round"
          strokeLinecap="round"
          points={line(actualPoints)}
        />
        {predictedPoints.map((point, index) => (
          <g key={`pred-${index}`}>
            <circle
              cx={point.x}
              cy={point.y}
              r="10"
              fill="transparent"
              className="perf-line-hover-target"
              onMouseEnter={() => setHoveredChartPoint(index, point.x, point.y)}
              onMouseMove={() => setHoveredChartPoint(index, point.x, point.y)}
            />
            <circle
              cx={point.x}
              cy={point.y}
              r="2.8"
              fill="#8b97a1"
              pointerEvents="none"
            />
          </g>
        ))}
        {actualPoints.map((point, index) => (
          <g key={`act-${index}`}>
            <circle
              cx={point.x}
              cy={point.y}
              r="10"
              fill="transparent"
              className="perf-line-hover-target"
              onMouseEnter={() => setHoveredChartPoint(index, point.x, point.y)}
              onMouseMove={() => setHoveredChartPoint(index, point.x, point.y)}
            />
            <circle
              cx={point.x}
              cy={point.y}
              r="2.8"
              fill="#63c768"
              pointerEvents="none"
            />
          </g>
        ))}
        {rows.map((game, index) =>
          index % labelEvery === 0 || index === rows.length - 1 ? (
            <text
              key={`label-${game.game_date}-${index}`}
              x={predictedPoints[index]?.x ?? 0}
              y={height - 4}
              textAnchor="middle"
              className="perf-trend-x-label"
            >
              {formatGameDateShortByRegion(game.game_date, userRegion)}
            </text>
          ) : null
        )}
      </svg>
      <div className="perf-chart-summary">Predicted vs actual over recent completed games, in {unit}.</div>
    </div>
  );
}

export default function PerformancePage() {
  const [userRegion, setUserRegion] = useState<UserRegion>("au");
  const [statType, setStatType] = useState<StatType>("points");
  const [days, setDays] = useState<number>(30);
  const [playerSearch, setPlayerSearch] = useState("");
  const [selectedPlayerId, setSelectedPlayerId] = useState<number | null>(null);
  const [playerSortKey, setPlayerSortKey] = useState<PlayerSortKey>("average_miss");
  const [playerSortDirection, setPlayerSortDirection] = useState<SortDirection>("asc");
  const [playerPage, setPlayerPage] = useState<number>(1);

  const [overviewState, setOverviewState] = useState<ApiState<ReviewOverview>>(initialState);
  const [trendState, setTrendState] = useState<ApiState<ReviewTrendResponse>>(initialState);
  const [playersState, setPlayersState] = useState<ApiState<ReviewPlayersResponse>>(initialState);
  const [recentState, setRecentState] = useState<ApiState<ReviewRecentResponse>>(initialState);
  const [playerDetailState, setPlayerDetailState] = useState<ApiState<ReviewPlayerDetail>>(initialState);

  const regionConfig = REGION_CONFIG[userRegion];
  const statMeta = STAT_OPTIONS.find((option) => option.value === statType) ?? STAT_OPTIONS[0];

  useEffect(() => {
    void Promise.all([
      safeLoad(setOverviewState, () => getReviewOverview({ stat_type: statType, days })),
      safeLoad(setTrendState, () => getReviewTrend({ stat_type: statType, days })),
      safeLoad(setPlayersState, () =>
        getReviewPlayers({ stat_type: statType, days, limit: 40, search: playerSearch || undefined })
      ),
      safeLoad(setRecentState, () =>
        getReviewRecent({ stat_type: statType, days: Math.min(days, 30), limit: 24 })
      ),
    ]).catch(() => undefined);
  }, [statType, days, playerSearch]);

  useEffect(() => {
    const firstPlayerId = playersState.data?.players?.[0]?.player_id ?? null;
    if (selectedPlayerId === null && firstPlayerId !== null) {
      setSelectedPlayerId(firstPlayerId);
    }
    if (
      selectedPlayerId !== null &&
      playersState.data?.players &&
      !playersState.data.players.some((player) => player.player_id === selectedPlayerId)
    ) {
      setSelectedPlayerId(firstPlayerId);
    }
  }, [playersState.data, selectedPlayerId]);

  useEffect(() => {
    if (selectedPlayerId === null) return;
    void safeLoad(setPlayerDetailState, () =>
      getReviewPlayerDetail(selectedPlayerId, { stat_type: statType, days: Math.max(days, 30) })
    ).catch(() => undefined);
  }, [selectedPlayerId, statType, days]);

  useEffect(() => {
    setPlayerPage(1);
  }, [statType, days, playerSearch, playerSortKey, playerSortDirection]);

  const sortedPlayers = useMemo(() => {
    const players = [...(playersState.data?.players ?? [])];
    players.sort((a, b) => {
      const dir = playerSortDirection === "asc" ? 1 : -1;
      if (playerSortKey === "full_name") {
        return a.full_name.localeCompare(b.full_name) * dir;
      }
      if (playerSortKey === "trend_label") {
        return (a.trend_label ?? "").localeCompare(b.trend_label ?? "") * dir;
      }
      const av = Number(a[playerSortKey] ?? 0);
      const bv = Number(b[playerSortKey] ?? 0);
      return (av - bv) * dir;
    });
    return players;
  }, [playersState.data?.players, playerSortDirection, playerSortKey]);

  const pagedPlayers = useMemo(() => {
    const start = (playerPage - 1) * PLAYER_PAGE_SIZE;
    return sortedPlayers.slice(start, start + PLAYER_PAGE_SIZE);
  }, [playerPage, sortedPlayers]);

  const totalPlayerPages = Math.max(1, Math.ceil(sortedPlayers.length / PLAYER_PAGE_SIZE));

  const selectedPlayer = useMemo(
    () =>
      sortedPlayers.find((player) => player.player_id === selectedPlayerId) ??
      sortedPlayers[0] ??
      null,
    [sortedPlayers, selectedPlayerId]
  );

  const strongestPlayer = sortedPlayers[0] ?? null;
  const shakiestPlayer = sortedPlayers.length > 0 ? sortedPlayers[sortedPlayers.length - 1] : null;

  function handlePlayerSort(nextKey: PlayerSortKey) {
    if (playerSortKey === nextKey) {
      setPlayerSortDirection((prev) => (prev === "asc" ? "desc" : "asc"));
      return;
    }
    setPlayerSortKey(nextKey);
    setPlayerSortDirection("asc");
  }

  return (
    <div className="app-shell min-vh-100 perf-shell">
      <header className="hero-header position-relative overflow-hidden">
        <div className="hero-glow"></div>
        <div className="container-fluid px-4 px-xxl-5 position-relative">
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
                <span className="nav-separator">|</span>
                <a className="nav-link nav-link-top text-white opacity-9 px-0 py-1" href="#about">
                  About
                </a>
              </div>
              <div className="d-flex align-items-center gap-2">
                <label className="text-xs text-white opacity-8 mb-0" htmlFor="performance-region-select">
                  Region
                </label>
                <select
                  id="performance-region-select"
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
                <p className="hero-slogan mb-3">How We&apos;re Doing</p>
                <h1 className="display-4 text-white mb-3">See how our predictions have actually been performing.</h1>
                <p className="lead text-white opacity-8 mb-4">
                  This page shows how close our predictions have been to the real NBA results,
                  who has been reliable, and where the model has been running hot or cold.
                </p>
                <p className="text-sm text-white opacity-8 mb-0">
                  Track recent accuracy, spot stronger reads, and dig into player-by-player performance.
                </p>
              </div>
            </div>
          </div>
        </div>
      </header>

      <section className="dashboard-section py-5">
        <div className="container-fluid px-4 px-xxl-5">
          <main className="perf-main">
            <section className="perf-hero">
              <div className="perf-hero-copy">
                <p className="perf-kicker">Prediction performance</p>
                <h2>A simple look at how our predictions have actually been landing.</h2>
                <p>{getPerformanceHeadline(overviewState.data)}</p>
                <div className="perf-meta">
                  <span>
                    Last fully updated on{" "}
                    <strong>{formatDateByRegion(overviewState.data?.updated_through, regionConfig)}</strong>
                  </span>
                  <span>Today&apos;s games can stay incomplete until the nightly refresh finishes.</span>
                </div>
              </div>
              <div className="perf-hero-controls">
                <label>
                  Stat
                  <select value={statType} onChange={(e) => setStatType(e.target.value as StatType)}>
                    {STAT_OPTIONS.map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  Window
                  <select value={days} onChange={(e) => setDays(Number(e.target.value))}>
                    {DAY_OPTIONS.map((value) => (
                      <option key={value} value={value}>
                        Last {value} days
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  Player search
                  <input
                    value={playerSearch}
                    onChange={(e) => setPlayerSearch(e.target.value)}
                    placeholder="Search player name"
                  />
                </label>
              </div>
            </section>

            {[overviewState.error, trendState.error, playersState.error, recentState.error, playerDetailState.error]
              .filter(Boolean)
              .slice(0, 1)
              .map((error) => (
                <div key={error} className="perf-empty-chart">
                  {error}
                </div>
              ))}

            <section className="perf-layout">
  <div className="perf-primary">
    <section className="perf-two-col perf-two-col-wide perf-player-zone">
      <article className="perf-panel perf-player-list-panel">
        <div className="perf-panel-head">
          <div>
            <p className="perf-kicker">Most reliable players</p>
            <h3>Who has the model handled best in this stat?</h3>
          </div>
          <span className="perf-panel-hint">Search a player or tap any row for a deeper look.</span>
        </div>
        <div className="perf-player-table">
          <div className="perf-player-table-head perf-player-table-head-buttons">
            <button type="button" onClick={() => handlePlayerSort("full_name")}>Player</button>
            <button type="button" onClick={() => handlePlayerSort("average_miss")}>Avg miss</button>
            <button type="button" onClick={() => handlePlayerSort("close_rate")}>Close rate</button>
            <button type="button" onClick={() => handlePlayerSort("trend_label")}>Trend</button>
          </div>
          {pagedPlayers.map((player) => (
            <button
              key={player.player_id}
              type="button"
              className={`perf-player-row ${getPlayerRowToneClass(player.average_miss, statType)} ${selectedPlayerId === player.player_id ? "active" : ""}`}
              onClick={() => setSelectedPlayerId(player.player_id)}
            >
              <div>
                <div className="perf-player-ident">
                  <div className="perf-recent-avatar perf-player-avatar">
                    {getHeadshotUrl(player.player_id, player.team_abbreviation) ? (
                      <img
                        src={getHeadshotUrl(player.player_id, player.team_abbreviation)}
                        alt={player.full_name}
                        onError={(e) => {
                          e.currentTarget.style.display = "none";
                        }}
                      />
                    ) : null}
                    <span className="perf-recent-avatar-fallback">{getInitials(player.full_name)}</span>
                  </div>
                  <div>
                    <strong>{player.full_name}</strong>
                    <small>
                      {player.team_abbreviation} - {player.reliability_tag}
                    </small>
                  </div>
                </div>
              </div>
              <span>{formatNumber(player.average_miss)} {statMeta.unit}</span>
              <span>{formatPercent(player.close_rate)}</span>
              <span>{player.trend_label}</span>
            </button>
          ))}
          {!playersState.loading && sortedPlayers.length === 0 && (
            <div className="perf-empty-chart">No player history found for this filter yet.</div>
          )}
        </div>
        <div className="perf-player-table-pagination">
          <button
            type="button"
            className="perf-page-btn"
            onClick={() => setPlayerPage((page) => Math.max(1, page - 1))}
            disabled={playerPage <= 1}
          >
            Prev
          </button>
          <span>Page {playerPage} of {totalPlayerPages}</span>
          <button
            type="button"
            className="perf-page-btn"
            onClick={() => setPlayerPage((page) => Math.min(totalPlayerPages, page + 1))}
            disabled={playerPage >= totalPlayerPages}
          >
            Next
          </button>
        </div>
      </article>

      <article className="perf-panel perf-player-focus">
        <div className="perf-panel-head">
          <div>
            <p className="perf-kicker">Player deep dive</p>
            <div className="perf-deepdive-ident">
              <div className="perf-recent-avatar perf-player-avatar perf-deepdive-avatar">
                {selectedPlayer && getHeadshotUrl(selectedPlayer.player_id, selectedPlayer.team_abbreviation) ? (
                  <img
                    src={getHeadshotUrl(selectedPlayer.player_id, selectedPlayer.team_abbreviation)}
                    alt={selectedPlayer.full_name}
                    onError={(e) => {
                      e.currentTarget.style.display = "none";
                    }}
                  />
                ) : null}
                <span className="perf-recent-avatar-fallback">
                  {getInitials(selectedPlayer?.full_name)}
                </span>
              </div>
              <h3>{selectedPlayer?.full_name ?? "Choose a player"}</h3>
            </div>
          </div>
          <span className="perf-panel-hint">
            {selectedPlayer
              ? `${selectedPlayer.team_abbreviation} - ${selectedPlayer.reliability_tag}`
              : "Player detail will appear here."}
          </span>
        </div>
        {playerDetailState.loading ? (
          <div className="perf-empty-chart">Loading player detail…</div>
        ) : playerDetailState.data ? (
          <>
            <p className="perf-player-story">
              {renderPlayerDirectionCopy(playerDetailState.data, statMeta.unit)}
            </p>
            <div className="perf-mini-cards">
              <div>
                <span>Average miss</span>
                <strong>{formatNumber(playerDetailState.data.average_miss)} {statMeta.unit}</strong>
              </div>
              <div>
                <span>Close rate</span>
                <strong>{formatPercent(playerDetailState.data.close_rate)}</strong>
              </div>
              <div>
                <span>Tends to run</span>
                <strong>
                  {playerDetailState.data.bias && playerDetailState.data.bias > 0
                    ? "High"
                    : playerDetailState.data.bias && playerDetailState.data.bias < 0
                      ? "Low"
                      : "Balanced"}
                </strong>
              </div>
            </div>
            <div className="perf-player-chart">
              <div className="perf-player-chart-head">
                <h4>
                  {selectedPlayer?.full_name ?? "Player"} - {statMeta.label}
                </h4>
              </div>
              <div className="perf-chart-legend perf-chart-legend-top">
                <span><i className="legend-dark" /> Predicted</span>
                <span><i className="legend-green" /> Actual</span>
              </div>
              <LineComparisonChart
                games={playerDetailState.data.games}
                unit={statMeta.unit}
                userRegion={userRegion}
              />
            </div>
            <div className="perf-history-table-wrap">
              <div className="perf-history-table-head">
                <span>Date</span>
                <span>Matchup</span>
                <span>Min</span>
                <span>What we predicted</span>
                <span>Actual</span>
                <span className="perf-history-head-miss">Miss</span>
              </div>
              <div className="perf-history-list perf-history-list-compact">
                {playerDetailState.data.games.map((game) => (
                  <div key={`${game.game_date}-${game.matchup}`} className="perf-history-row">
                    <div className="perf-history-cell perf-history-date">
                      {formatGameDateByRegion(game.game_date, regionConfig, userRegion)}
                    </div>
                    <div className="perf-history-cell perf-history-matchup">
                      {game.matchup || "Matchup not available"}
                    </div>
                    <div className="perf-history-cell">
                      {typeof game.minutes === "number" && Number.isFinite(game.minutes)
                        ? Math.round(game.minutes)
                        : "-"}
                    </div>
                    <div className="perf-history-cell">
                      {formatNumber(game.predicted)} {statMeta.unit}
                    </div>
                    <div className="perf-history-cell">
                      {isDnpGame(game) ? "DNP" : `${formatNumber(game.actual)} ${statMeta.unit}`}
                    </div>
                    <div
                      className={`perf-history-cell perf-history-delta ${getSignedDeltaToneClass(
                        !isDnpGame(game) &&
                          typeof game.actual === "number" &&
                          Number.isFinite(game.actual) &&
                          typeof game.predicted === "number" &&
                          Number.isFinite(game.predicted)
                          ? game.actual - game.predicted
                          : null
                      )}`}
                    >
                      {isDnpGame(game)
                        ? "DNP"
                        : `${formatSignedNumber(
                        typeof game.actual === "number" &&
                          Number.isFinite(game.actual) &&
                          typeof game.predicted === "number" &&
                          Number.isFinite(game.predicted)
                          ? game.actual - game.predicted
                          : null
                      )} ${statMeta.unit}`}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </>
        ) : (
          <div className="perf-empty-chart">Choose a player with completed history.</div>
        )}
      </article>
    </section>
  </div>

  <aside className="perf-sidebar">
    <section className="perf-card-grid perf-card-grid-vertical">
      <article className="perf-stat-card tone-neutral">
        <span className="perf-card-label">Predictions tracked</span>
        <strong>{overviewState.data?.tracked_predictions ?? "-"}</strong>
        <p>Completed {statMeta.label.toLowerCase()} results in this window.</p>
      </article>
      <article className={`perf-stat-card ${getBiasDirectionToneClass(overviewState.data?.bias)}`}>
        <span className="perf-card-label">Average drift</span>
        <div className="perf-stat-value-meta">
          <strong className={getBiasValueToneClass(overviewState.data?.bias)}>
            {formatSignedNumber(overviewState.data?.bias)} {statMeta.unit}
          </strong>
          <em>{getBiasDirectionLabel(overviewState.data?.bias)}</em>
        </div>
        <p>{getBiasDirectionExplanation(overviewState.data?.bias, statMeta.unit)}</p>
      </article>
      <article className="perf-stat-card tone-neutral">
        <span className="perf-card-label">Overall read</span>
        <strong>{getMissReadLabel(overviewState.data?.average_miss, statType)}</strong>
        <p>
          {getMissReadLabel(overviewState.data?.average_miss, statType) === "Good"
            ? "The model has been landing fairly close to the real result in this window."
            : getMissReadLabel(overviewState.data?.average_miss, statType) === "Mixed"
              ? "There have been solid calls, but the misses have been uneven."
              : "The gap to the real result has been wider than you would want."}
        </p>
      </article>
      <article className="perf-stat-card tone-neutral">
        <span className="perf-card-label">Close-call rate</span>
        <strong>{formatPercent(overviewState.data?.close_rate)}</strong>
        <p>
          {getCloseCallExplanation(
            overviewState.data?.close_call_threshold,
            statMeta.unit,
            statMeta.label
          )}
        </p>
      </article>
                </section>

    <article className="perf-panel perf-trend-panel">
      <div className="perf-panel-head perf-trend-panel-head">
        <div>
          <p className="perf-kicker">Recent accuracy trend</p>
          <h3>Has the model been getting closer or drifting away?</h3>
        </div>
        <span className="perf-panel-hint perf-trend-hint">
          Lower bars are better. Green is stronger, yellow is mixed, red is weaker.
        </span>
      </div>
      <TrendChart
        points={trendState.data?.points ?? []}
        regionConfig={regionConfig}
        userRegion={userRegion}
        statType={statType}
      />
    </article>

    <article className="perf-panel perf-panel-story" id="about">
      <div className="perf-panel-head">
        <div>
          <p className="perf-kicker">Current read</p>
          <h3>What stands out right now</h3>
        </div>
      </div>
      <div className="perf-story-list">
        <div className="perf-story-pill">
          <span>Overall</span>
          <strong>{overviewState.data?.story ?? "Still building enough history to comment confidently."}</strong>
        </div>
        <div className="perf-story-pill">
          <span>Most reliable</span>
          <strong>
            {strongestPlayer
              ? `${strongestPlayer.full_name} has been one of the steadier ${statMeta.label.toLowerCase()} reads based on the average miss in this time window.`
              : "No player summary yet."}
          </strong>
        </div>
        <div className="perf-story-pill">
          <span>Watch list</span>
          <strong>
            {shakiestPlayer
              ? `${shakiestPlayer.full_name} has been more volatile lately, based on a wider average miss than the rest of the tracked group.`
              : "No volatility signal yet."}
          </strong>
        </div>
      </div>
    </article>
  </aside>
</section>
<section className="perf-panel">
              <div className="perf-panel-head">
                <div>
                  <p className="perf-kicker">Recent results</p>
                  <h3>How the latest predictions stacked up against reality</h3>
                </div>
                <span className="perf-panel-hint">
                  Green means the prediction landed close. Yellow is middling. Red means it missed by more.
                </span>
              </div>
              <div className="perf-recent-grid">
                {(recentState.data?.results ?? []).map((row) => {
                  const tone = getResultTone(row.average_miss, overviewState.data?.close_call_threshold);
                  const predicted = row.predicted ?? 0;
                  const actual = row.actual ?? 0;
                  const maxValue = Math.max(predicted, actual, 1);
                  const headshotUrl = getHeadshotUrl(row.player_id, row.team_abbreviation);
                  return (
                    <article key={`${row.player_id}-${row.game_date}-${row.matchup}`} className={`perf-recent-card tone-${tone}`}>
                      <div className="perf-recent-head">
                        <div className="perf-recent-player">
                          <div className="perf-recent-avatar">
                            {headshotUrl ? (
                              <img
                                src={headshotUrl}
                                alt={row.full_name}
                                onError={(e) => {
                                  e.currentTarget.style.display = "none";
                                }}
                              />
                            ) : null}
                            <span className="perf-recent-avatar-fallback">{getInitials(row.full_name)}</span>
                          </div>
                          <div>
                            <strong>{row.full_name}</strong>
                            <small>
                              {formatGameDateByRegion(row.game_date, regionConfig, userRegion)} - {row.matchup || row.team_abbreviation}
                              {typeof row.minutes === "number" ? ` - ${Math.round(row.minutes)} min` : ""}
                            </small>
                          </div>
                        </div>
                        <span>{row.result_label}</span>
                      </div>
                      <div className="perf-compare">
                        <div>
                          <label>Predicted</label>
                          <div className="perf-compare-bar">
                            <span style={{ width: `${(predicted / maxValue) * 100}%` }} />
                          </div>
                          <strong>{formatNumber(predicted)} {statMeta.unit}</strong>
                        </div>
                        <div>
                          <label>Actual</label>
                          <div className="perf-compare-bar perf-compare-bar-actual">
                            <span style={{ width: `${(actual / maxValue) * 100}%`, background: getActualFillColor(predicted, actual, overviewState.data?.close_call_threshold) }} />
                          </div>
                          <strong>{formatNumber(actual)} {statMeta.unit}</strong>
                        </div>
                      </div>
                      <p>
                        Missed by <strong>{formatNumber(row.average_miss)} {statMeta.unit}</strong>
                        {typeof row.confidence === "number" ? ` - confidence ${Math.round(row.confidence)}%` : ""}
                      </p>
                    </article>
                  );
                })}
                {!recentState.loading && (recentState.data?.results?.length ?? 0) === 0 && (
                  <div className="perf-empty-chart">No recent completed results in this window yet.</div>
                )}
              </div>
            </section>
          </main>
        </div>
      </section>
    </div>
  );
}
