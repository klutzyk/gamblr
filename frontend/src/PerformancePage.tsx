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

function formatNumber(value: number | null | undefined, digits = 1) {
  return typeof value === "number" && Number.isFinite(value) ? value.toFixed(digits) : "-";
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

function getAverageMissExplanation(value: number | null | undefined, unit: string) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "Not enough completed results yet.";
  }
  return `On average, the prediction finished about ${value.toFixed(1)} ${unit} above or below the real result. Lower is better.`;
}

function getBiasText(value: number | null | undefined, unit: string) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "Not enough completed results yet.";
  }
  if (value >= 1) return `Usually about ${Math.abs(value).toFixed(1)} ${unit} too high.`;
  if (value <= -1) return `Usually about ${Math.abs(value).toFixed(1)} ${unit} too low.`;
  return `Usually within ${Math.abs(value).toFixed(1)} ${unit} of balanced.`;
}

function getBiasToneClass(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "tone-neutral";
  if (value >= 1) return "tone-warm";
  if (value <= -1) return "tone-cool";
  return "tone-good";
}

function getTrendToneClass(label: string | null | undefined) {
  if (!label) return "tone-neutral";
  const lower = label.toLowerCase();
  if (lower.includes("improving")) return "tone-good";
  if (lower.includes("cooling")) return "tone-warm";
  return "tone-neutral";
}

function getResultTone(bias: number | null | undefined) {
  if (typeof bias !== "number") return "neutral";
  if (bias > 0.75) return "high";
  if (bias < -0.75) return "low";
  return "neutral";
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

function Sparkline({
  values,
  stroke,
}: {
  values: Array<number | null | undefined>;
  stroke: string;
}) {
  const numeric = values.filter(
    (value): value is number => typeof value === "number" && Number.isFinite(value)
  );
  if (numeric.length < 2) {
    return <div className="perf-empty-chart">Not enough data yet</div>;
  }

  const min = Math.min(...numeric);
  const max = Math.max(...numeric);
  const range = Math.max(max - min, 0.001);
  const points = numeric.map((value, index) => {
    const x = (index / Math.max(numeric.length - 1, 1)) * 100;
    const y = 100 - ((value - min) / range) * 100;
    return `${x},${y}`;
  });

  return (
    <svg viewBox="0 0 100 100" className="perf-sparkline" preserveAspectRatio="none">
      <polyline
        fill="none"
        stroke={stroke}
        strokeWidth="3"
        strokeLinejoin="round"
        strokeLinecap="round"
        points={points.join(" ")}
      />
    </svg>
  );
}

function TrendBars({
  points,
  regionConfig,
}: {
  points: ReviewTrendResponse["points"];
  regionConfig: (typeof REGION_CONFIG)[UserRegion];
}) {
  const visible = points.slice(-14);
  const maxMiss = Math.max(...visible.map((point) => point.average_miss ?? 0), 1);
  return (
    <div className="perf-trend-wrap">
      {visible.length === 0 ? (
        <div className="perf-empty-chart">No completed days in this window yet.</div>
      ) : (
        visible.map((point) => {
          const miss = point.average_miss ?? 0;
          const height = Math.max(8, (miss / maxMiss) * 100);
          const closeRate = point.close_rate ?? 0;
          return (
            <div key={point.game_date} className="perf-trend-bar">
              <div
                className="perf-trend-fill"
                style={{ height: `${height}%`, opacity: 0.35 + closeRate * 0.65 }}
                title={`${formatDateByRegion(point.game_date, regionConfig)} • Avg miss ${formatNumber(miss)}`}
              />
              <span>{formatDateByRegion(point.game_date, regionConfig)}</span>
            </div>
          );
        })
      )}
    </div>
  );
}

export default function PerformancePage() {
  const [userRegion, setUserRegion] = useState<UserRegion>("au");
  const [statType, setStatType] = useState<StatType>("points");
  const [days, setDays] = useState<number>(30);
  const [playerSearch, setPlayerSearch] = useState("");
  const [selectedPlayerId, setSelectedPlayerId] = useState<number | null>(null);

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

  const selectedPlayer = useMemo(
    () =>
      playersState.data?.players.find((player) => player.player_id === selectedPlayerId) ??
      playersState.data?.players?.[0] ??
      null,
    [playersState.data, selectedPlayerId]
  );

  const strongestPlayer = playersState.data?.players?.[0] ?? null;
  const shakiestPlayer =
    playersState.data?.players && playersState.data.players.length > 0
      ? playersState.data.players[playersState.data.players.length - 1]
      : null;

  return (
    <div className="app-shell min-vh-100 perf-shell">
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
        <div className="container">
          <main className="perf-main">
            <section className="perf-hero">
              <div className="perf-hero-copy">
                <p className="perf-kicker">Prediction performance</p>
                <h2>A simple look at how our predictions have actually been landing.</h2>
                <p>{getPerformanceHeadline(overviewState.data)}</p>
                <div className="perf-meta">
                  <span>
                    Last fully updated through{" "}
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

            <section className="perf-card-grid">
              <article className="perf-stat-card tone-neutral">
                <span className="perf-card-label">Predictions tracked</span>
                <strong>{overviewState.data?.tracked_predictions ?? "-"}</strong>
                <p>Completed {statMeta.label.toLowerCase()} results in this window.</p>
              </article>
              <article className="perf-stat-card tone-neutral">
                <span className="perf-card-label">Average miss</span>
                <strong>{formatNumber(overviewState.data?.average_miss)} {statMeta.unit}</strong>
                <p>{getAverageMissExplanation(overviewState.data?.average_miss, statMeta.unit)}</p>
              </article>
              <article className="perf-stat-card tone-neutral">
                <span className="perf-card-label">Typical miss</span>
                <strong>{formatNumber(overviewState.data?.median_miss)} {statMeta.unit}</strong>
                <p>This is the more typical miss, without being skewed by a few wild results.</p>
              </article>
              <article className="perf-stat-card tone-good">
                <span className="perf-card-label">Close-call rate</span>
                <strong>{formatPercent(overviewState.data?.close_rate)}</strong>
                <p>Share of tracked picks that landed in a tight range around the real result.</p>
              </article>
              <article className={`perf-stat-card ${getBiasToneClass(overviewState.data?.bias)}`}>
                <span className="perf-card-label">Tends to run</span>
                <strong>{overviewState.data?.bias_label ?? "-"}</strong>
                <p>{getBiasText(overviewState.data?.bias, statMeta.unit)}</p>
              </article>
              <article className={`perf-stat-card perf-stat-card-accent ${getTrendToneClass(overviewState.data?.recent_trend_label)}`}>
                <span className="perf-card-label">Recent trend</span>
                <strong>{overviewState.data?.recent_trend_label ?? "-"}</strong>
                <Sparkline
                  values={trendState.data?.points.map((point) => point.average_miss) ?? []}
                  stroke="#6bd06f"
                />
              </article>
            </section>

            <section className="perf-two-col">
              <article className="perf-panel">
                <div className="perf-panel-head">
                  <div>
                    <p className="perf-kicker">Recent accuracy trend</p>
                    <h3>Has the model been getting closer or drifting away?</h3>
                  </div>
                  <span className="perf-panel-hint">Lower bars mean the predictions were closer that day.</span>
                </div>
                <TrendBars points={trendState.data?.points ?? []} regionConfig={regionConfig} />
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
            </section>

            <section className="perf-two-col perf-two-col-wide">
              <article className="perf-panel">
                <div className="perf-panel-head">
                  <div>
                    <p className="perf-kicker">Most reliable players</p>
                    <h3>Who has the model handled best in this stat?</h3>
                  </div>
                  <span className="perf-panel-hint">Tap any player for a deeper look.</span>
                </div>
                <div className="perf-player-table">
                  <div className="perf-player-table-head">
                    <span>Player</span>
                    <span>Average miss</span>
                    <span>Close rate</span>
                    <span>Trend</span>
                  </div>
                  {(playersState.data?.players ?? []).map((player) => (
                    <button
                      key={player.player_id}
                      type="button"
                      className={`perf-player-row ${selectedPlayerId === player.player_id ? "active" : ""}`}
                      onClick={() => setSelectedPlayerId(player.player_id)}
                    >
                      <div>
                        <strong>{player.full_name}</strong>
                        <small>
                          {player.team_abbreviation} · {player.reliability_tag}
                        </small>
                      </div>
                      <span>{formatNumber(player.average_miss)} {statMeta.unit}</span>
                      <span>{formatPercent(player.close_rate)}</span>
                      <span>{player.trend_label}</span>
                    </button>
                  ))}
                  {!playersState.loading && (playersState.data?.players?.length ?? 0) === 0 && (
                    <div className="perf-empty-chart">No player history found for this filter yet.</div>
                  )}
                </div>
              </article>

              <article className="perf-panel perf-player-focus">
                <div className="perf-panel-head">
                  <div>
                    <p className="perf-kicker">Player deep dive</p>
                    <h3>{selectedPlayer?.full_name ?? "Choose a player"}</h3>
                  </div>
                  <span className="perf-panel-hint">
                    {selectedPlayer
                      ? `${selectedPlayer.team_abbreviation} · ${selectedPlayer.reliability_tag}`
                      : "Player detail will appear here."}
                  </span>
                </div>
                {playerDetailState.loading ? (
                  <div className="perf-empty-chart">Loading player detail…</div>
                ) : playerDetailState.data ? (
                  <>
                    <p className="perf-player-story">{playerDetailState.data.story}</p>
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
                      <Sparkline
                        values={[...(playerDetailState.data.games ?? [])].reverse().map((game) => game.predicted)}
                        stroke="#102a1c"
                      />
                      <Sparkline
                        values={[...(playerDetailState.data.games ?? [])].reverse().map((game) => game.actual)}
                        stroke="#6bd06f"
                      />
                      <div className="perf-chart-legend">
                        <span><i className="legend-dark" /> Predicted</span>
                        <span><i className="legend-green" /> Actual</span>
                      </div>
                    </div>
                    <div className="perf-history-list">
                      {playerDetailState.data.games.map((game) => (
                        <div key={`${game.game_date}-${game.matchup}`} className="perf-history-row">
                          <div>
                            <strong>{formatDateByRegion(game.game_date, regionConfig)}</strong>
                            <small>{game.matchup || "Matchup not available"}</small>
                          </div>
                          <div>
                            <span>Predicted</span>
                            <strong>{formatNumber(game.predicted)} {statMeta.unit}</strong>
                          </div>
                          <div>
                            <span>Actual</span>
                            <strong>{formatNumber(game.actual)} {statMeta.unit}</strong>
                          </div>
                          <div>
                            <span>Miss</span>
                            <strong>{formatNumber(game.average_miss)} {statMeta.unit}</strong>
                          </div>
                        </div>
                      ))}
                    </div>
                  </>
                ) : (
                  <div className="perf-empty-chart">Choose a player with completed history.</div>
                )}
              </article>
            </section>

            <section className="perf-panel">
              <div className="perf-panel-head">
                <div>
                  <p className="perf-kicker">Recent results</p>
                  <h3>How the latest predictions stacked up against reality</h3>
                </div>
                <span className="perf-panel-hint">Green means the prediction came in close. Red means it ran further off.</span>
              </div>
              <div className="perf-recent-grid">
                {(recentState.data?.results ?? []).map((row) => {
                  const tone = getResultTone(row.bias);
                  const predicted = row.predicted ?? 0;
                  const actual = row.actual ?? 0;
                  const maxValue = Math.max(predicted, actual, 1);
                  return (
                    <article key={`${row.player_id}-${row.game_date}-${row.matchup}`} className={`perf-recent-card tone-${tone}`}>
                      <div className="perf-recent-head">
                        <div>
                          <strong>{row.full_name}</strong>
                          <small>
                            {formatDateByRegion(row.game_date, regionConfig)} · {row.matchup || row.team_abbreviation}
                          </small>
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
                            <span style={{ width: `${(actual / maxValue) * 100}%` }} />
                          </div>
                          <strong>{formatNumber(actual)} {statMeta.unit}</strong>
                        </div>
                      </div>
                      <p>
                        Missed by <strong>{formatNumber(row.average_miss)} {statMeta.unit}</strong>
                        {typeof row.confidence === "number" ? ` · confidence ${Math.round(row.confidence)}%` : ""}
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
