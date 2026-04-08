export type PlayerRow = {
  PLAYER_ID: number;
  PLAYER_NAME: string;
  TEAM_ABBREVIATION?: string;
  GP?: number;
  MIN?: number;
  PTS?: number;
  REB?: number;
  AST?: number;
  NBA_FANTASY_PTS?: number;
  [key: string]: unknown;
};

export type PlayerPropsResponse = {
  game_id: number;
  markets: unknown[];
  count: number;
};

export type OddsEvent = {
  id: string;
  sport_key: string;
  sport_title: string;
  commence_time: string;
  home_team: string;
  away_team: string;
};

export type OddsEventOutcome = {
  name: string;
  description: string;
  price: number;
  point?: number;
};

export type OddsEventMarket = {
  key: string;
  last_update: string;
  outcomes: OddsEventOutcome[];
};

export type OddsEventBookmaker = {
  key: string;
  title: string;
  markets: OddsEventMarket[];
};

export type OddsEventPropsPayload = {
  id: string;
  sport_key: string;
  sport_title: string;
  commence_time: string;
  home_team: string;
  away_team: string;
  bookmakers: OddsEventBookmaker[];
};

export type OddsEventPropsResponse = {
  data: OddsEventPropsPayload;
  usage?: {
    requests_last?: number | null;
    requests_remaining?: number | null;
    requests_used?: number | null;
  };
  estimated_cost?: number;
};

export type OddsUsageSnapshot = {
  requests_last?: number | null;
  requests_remaining?: number | null;
  requests_used?: number | null;
};

export type PredictionRow = {
  player_id: number;
  full_name: string;
  team_id?: number;
  team_abbreviation: string;
  matchup: string;
  game_date: string;
  game_id?: string;
  tipoff_et?: string | null;
  tipoff_au?: string | null;
  pred_value: number;
  pred_p10?: number;
  pred_p50?: number;
  pred_p90?: number;
  confidence?: number;
  under_risk?: number | null;
  under_risk_n?: number | null;
  last_under_date?: string | null;
  last_under_value?: number | null;
  last_under_games_ago?: number | null;
  last_under_matchup?: string | null;
  last_under_minutes?: number | null;
  model_version?: string;
};

export type FirstBasketPredictionRow = {
  game_id?: string;
  game_date?: string;
  matchup: string;
  tipoff_et?: string;
  tipoff_au?: string;
  team_side?: string;
  team_id?: number | null;
  team_abbreviation: string;
  lineup_status?: string;
  player_id: number;
  full_name: string;
  position?: string;
  first_basket_prob: number;
  team_scores_first_prob?: number;
  player_share_on_team?: number;
  model_version?: string;
  jedibets_first_baskets?: number;
  jedibets_team_first_fg_pct?: number;
};

export type DoubleTriplePredictionRow = {
  player_id: number;
  full_name: string;
  team_id?: number;
  team_abbreviation: string;
  matchup: string;
  game_date: string;
  game_id?: string;
  pts_pred: number;
  reb_pred: number;
  ast_pred: number;
  double_double_prob: number;
  triple_double_prob: number;
  double_double_pct?: number;
  triple_double_pct?: number;
  p_pts_ge_10?: number;
  p_reb_ge_10?: number;
  p_ast_ge_10?: number;
};

export type BestBetLeg = {
  event_id: string;
  commence_time: string;
  matchup: string;
  bookmaker: string;
  market: string;
  stat_type: string;
  player_name: string;
  side: "Over" | "Under" | string;
  line: number;
  price_decimal: number;
  implied_prob: number;
  model_prob_raw: number;
  model_prob: number;
  edge: number;
  ev_per_unit: number;
  prediction: {
    pred_value?: number;
    pred_p10?: number;
    pred_p50?: number;
    pred_p90?: number;
    confidence?: number;
    team_abbreviation?: string;
  };
};

export type BestBetParlay = {
  legs: BestBetLeg[];
  leg_count?: number;
  combined_odds: number;
  combined_probability: number;
  expected_value_per_unit: number;
  meets_target: boolean;
};

export type BestBetsResponse = {
  status: string;
  generated_at?: string;
  bookmaker?: string;
  target_multiplier?: number;
  leg_count?: number;
  day?: string;
  pool_size?: number;
  top_single_legs?: BestBetLeg[];
  recommended_parlays?: BestBetParlay[];
  message?: string;
  debug?: Record<string, number>;
};

export type BestBetsProgress = {
  request_id?: string | null;
  status?: string | null;
  phase?: string | null;
  message?: string | null;
  current_matchup?: string | null;
  rows_processed?: number | null;
  rows_total?: number | null;
  candidates_kept?: number | null;
  combos_considered?: number | null;
  updated_at?: string | null;
};

export type ReviewOverview = {
  stat_type: string;
  stat_label: string;
  days: number;
  tracked_predictions: number;
  average_miss?: number | null;
  median_miss?: number | null;
  bias?: number | null;
  bias_label?: string | null;
  close_rate?: number | null;
  recent_avg_miss?: number | null;
  prior_avg_miss?: number | null;
  recent_trend_label?: string | null;
  updated_through?: string | null;
  story?: string | null;
};

export type ReviewTrendPoint = {
  game_date: string;
  prediction_count: number;
  average_miss?: number | null;
  bias?: number | null;
  close_rate?: number | null;
};

export type ReviewTrendResponse = {
  stat_type: string;
  days: number;
  points: ReviewTrendPoint[];
};

export type ReviewPlayerRow = {
  player_id: number;
  full_name: string;
  team_abbreviation: string;
  tracked_predictions: number;
  average_miss?: number | null;
  median_miss?: number | null;
  close_rate?: number | null;
  bias?: number | null;
  recent_avg_miss?: number | null;
  prior_avg_miss?: number | null;
  trend_label?: string | null;
  reliability_tag?: string | null;
  last_game_date?: string | null;
};

export type ReviewPlayersResponse = {
  stat_type: string;
  days: number;
  players: ReviewPlayerRow[];
};

export type ReviewRecentRow = {
  player_id: number;
  full_name: string;
  team_abbreviation: string;
  game_date: string;
  matchup: string;
  predicted?: number | null;
  actual?: number | null;
  average_miss?: number | null;
  confidence?: number | null;
  bias?: number | null;
  result_label?: string | null;
};

export type ReviewRecentResponse = {
  stat_type: string;
  days: number;
  results: ReviewRecentRow[];
};

export type ReviewPlayerGame = {
  game_date: string;
  matchup: string;
  predicted?: number | null;
  actual?: number | null;
  average_miss?: number | null;
  confidence?: number | null;
  bias?: number | null;
};

export type ReviewPlayerDetail = {
  player_id: number;
  full_name: string;
  team_abbreviation: string;
  stat_type: string;
  days: number;
  tracked_predictions: number;
  average_miss?: number | null;
  median_miss?: number | null;
  close_rate?: number | null;
  bias?: number | null;
  reliability_tag?: string | null;
  last_game_date?: string | null;
  story?: string | null;
  games: ReviewPlayerGame[];
};

const API_BASE =
  import.meta.env.VITE_API_BASE_URL ?? "http://127.0.0.1:8000";

// Very small in-memory cache to avoid hammering the backend
type CacheEntry<T> = { data: T; timestamp: number };
const cache: Record<string, CacheEntry<unknown>> = {};

function getCacheKey(path: string, params?: Record<string, unknown>): string {
  const p = params ? JSON.stringify(params) : "";
  return `${path}?${p}`;
}

async function fetchWithCache<T>(
  path: string,
  params: Record<string, unknown> | undefined,
  ttlMs: number
): Promise<T> {
  const key = getCacheKey(path, params);
  const now = Date.now();

  const existing = cache[key] as CacheEntry<T> | undefined;
  if (existing && now - existing.timestamp < ttlMs) {
    return existing.data;
  }

  const url = new URL(path, API_BASE);
  if (params) {
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== null && v !== "") {
        url.searchParams.set(k, String(v));
      }
    });
  }

  const res = await fetch(url.toString());
  if (!res.ok) {
    throw new Error(`Request failed with status ${res.status}`);
  }
  const data = (await res.json()) as T;
  cache[key] = { data, timestamp: now };
  return data;
}

async function postJson<T>(
  path: string,
  params?: Record<string, unknown>
): Promise<T> {
  const url = new URL(path, API_BASE);
  if (params) {
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== null && v !== "") {
        url.searchParams.set(k, String(v));
      }
    });
  }

  const res = await fetch(url.toString(), { method: "POST" });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Request failed with status ${res.status}: ${body}`);
  }
  return (await res.json()) as T;
}

// Player stats endpoints (cached clientside for 10 minutes)
const PLAYER_TTL = 10 * 60 * 1000;

export function getTopScorers(
  season = "2025-26",
  top_n = 10
): Promise<PlayerRow[]> {
  return fetchWithCache<PlayerRow[]>(
    "/players/top_scorers",
    { season, top_n },
    PLAYER_TTL
  );
}

export function getTopAssists(
  season = "2025-26",
  top_n = 10
): Promise<PlayerRow[]> {
  return fetchWithCache<PlayerRow[]>(
    "/players/top_assists",
    { season, top_n },
    PLAYER_TTL
  );
}

export function getTopRebounders(
  season = "2025-26",
  top_n = 10
): Promise<PlayerRow[]> {
  return fetchWithCache<PlayerRow[]>(
    "/players/top_rebounders",
    { season, top_n },
    PLAYER_TTL
  );
}

export function getGuardStats(
  season = "2025-26",
  top_n = 10
): Promise<PlayerRow[]> {
  return fetchWithCache<PlayerRow[]>(
    "/players/guards_stats",
    { season, top_n },
    PLAYER_TTL
  );
}

export function getRecentPerformers(
  season = "2025-26",
  last_n_games = 5,
  top_n = 10
): Promise<PlayerRow[]> {
  return fetchWithCache<PlayerRow[]>(
    "/players/recent_performers",
    { season, last_n_games, top_n },
    PLAYER_TTL
  );
}

// Player props  much stricter TTL (1 hour) because of low quota
const PROPS_TTL = 60 * 60 * 1000;

export function getPlayerPropsByGame(
  gameId: number
): Promise<PlayerPropsResponse> {
  return fetchWithCache<PlayerPropsResponse>(
    `/player-props/${gameId}`,
    undefined,
    PROPS_TTL
  );
}

export function getNbaEvents(): Promise<OddsEvent[]> {
  return fetchWithCache<OddsEvent[]>(
    "/odds/basketball_nba/events",
    undefined,
    5 * 60 * 1000
  );
}

export function getOddsUsageSnapshot(): Promise<OddsUsageSnapshot> {
  return fetchWithCache<OddsUsageSnapshot>("/odds/usage/snapshot", undefined, 30 * 1000);
}

export function getBestBetsProgress(): Promise<BestBetsProgress> {
  return fetchWithCache<BestBetsProgress>("/bets/best/progress", undefined, 1000);
}

export function getReviewOverview(params: {
  stat_type: string;
  days: number;
}): Promise<ReviewOverview> {
  return fetchWithCache<ReviewOverview>("/review/overview", params, 60 * 1000);
}

export function getReviewTrend(params: {
  stat_type: string;
  days: number;
}): Promise<ReviewTrendResponse> {
  return fetchWithCache<ReviewTrendResponse>("/review/trend", params, 60 * 1000);
}

export function getReviewPlayers(params: {
  stat_type: string;
  days: number;
  limit?: number;
  search?: string;
}): Promise<ReviewPlayersResponse> {
  return fetchWithCache<ReviewPlayersResponse>("/review/players", params, 60 * 1000);
}

export function getReviewRecent(params: {
  stat_type: string;
  days: number;
  limit?: number;
}): Promise<ReviewRecentResponse> {
  return fetchWithCache<ReviewRecentResponse>("/review/recent", params, 60 * 1000);
}

export function getReviewPlayerDetail(
  playerId: number,
  params: { stat_type: string; days: number }
): Promise<ReviewPlayerDetail> {
  return fetchWithCache<ReviewPlayerDetail>(`/review/player/${playerId}`, params, 60 * 1000);
}

export function getOddsEventProps(
  eventId: string,
  params: {
    markets?: string;
    bookmakers?: string;
    min_remaining_after_call?: number;
  } = {}
): Promise<OddsEventPropsResponse> {
  const resolved = {
    markets: "player_points,player_assists,player_rebounds",
    bookmakers: "sportsbet",
    ...params,
  };
  return fetchWithCache<OddsEventPropsResponse>(
    `/odds/basketball_nba/events/${eventId}/odds`,
    resolved,
    5 * 60 * 1000
  );
}

// Predictions endpoint (cached for 5 minutes since predictions may update)
const PREDICTIONS_TTL = 5 * 60 * 1000;

type PredictionResponsePayload = PredictionRow[] | { data?: PredictionRow[] };
export type PredictionDayParam =
  | "today"
  | "tomorrow"
  | "yesterday"
  | "two_days_ago"
  | "auto";

function normalizePredictionRows(payload: PredictionResponsePayload): PredictionRow[] {
  if (Array.isArray(payload)) {
    return payload;
  }
  if (payload && Array.isArray(payload.data)) {
    return payload.data;
  }
  return [];
}

export function getPointsPredictions(
  day: PredictionDayParam = "auto"
): Promise<PredictionRow[]> {
  return fetchWithCache<PredictionResponsePayload>(
    "/players/predictions/points",
    { day },
    PREDICTIONS_TTL
  ).then(normalizePredictionRows);
}

export function getAssistsPredictions(
  day: PredictionDayParam = "auto"
): Promise<PredictionRow[]> {
  return fetchWithCache<PredictionResponsePayload>(
    "/players/predictions/assists",
    { day },
    PREDICTIONS_TTL
  ).then(normalizePredictionRows);
}

export function getReboundsPredictions(
  day: PredictionDayParam = "auto"
): Promise<PredictionRow[]> {
  return fetchWithCache<PredictionResponsePayload>(
    "/players/predictions/rebounds",
    { day },
    PREDICTIONS_TTL
  ).then(normalizePredictionRows);
}

export function getThreeptPredictions(
  day: PredictionDayParam = "auto"
): Promise<PredictionRow[]> {
  return fetchWithCache<PredictionResponsePayload>(
    "/players/predictions/threept",
    { day },
    PREDICTIONS_TTL
  ).then(normalizePredictionRows);
}

export function getThreepaPredictions(
  day: PredictionDayParam = "auto"
): Promise<PredictionRow[]> {
  return fetchWithCache<PredictionResponsePayload>(
    "/players/predictions/threepa",
    { day },
    PREDICTIONS_TTL
  ).then(normalizePredictionRows);
}

export function getFirstBasketPredictions(
  day: PredictionDayParam = "auto",
  top_n_per_game = 6
): Promise<FirstBasketPredictionRow[]> {
  const url = new URL("/players/predictions/first_basket", API_BASE);
  url.searchParams.set("day", day);
  url.searchParams.set("top_n_per_game", String(top_n_per_game));
  // First-basket output is sensitive to lineup/time updates; skip client cache.
  url.searchParams.set("_t", String(Date.now()));
  return fetch(url.toString())
    .then(async (res) => {
      if (!res.ok) {
        throw new Error(`Request failed with status ${res.status}`);
      }
      return (await res.json()) as { data?: FirstBasketPredictionRow[] };
    })
    .then((payload) => payload.data ?? []);
}

export async function getApiHealth(): Promise<{ message?: string }> {
  const url = new URL("/", API_BASE);
  const res = await fetch(url.toString());
  if (!res.ok) {
    throw new Error(`Health check failed (${res.status})`);
  }
  return (await res.json()) as { message?: string };
}

type DoubleTripleResponsePayload =
  | DoubleTriplePredictionRow[]
  | { data?: DoubleTriplePredictionRow[] };

function normalizeDoubleTripleRows(
  payload: DoubleTripleResponsePayload
): DoubleTriplePredictionRow[] {
  if (Array.isArray(payload)) {
    return payload;
  }
  if (payload && Array.isArray(payload.data)) {
    return payload.data;
  }
  return [];
}

export function getDoublesPredictions(
  day: PredictionDayParam = "auto",
  top_n = 30
): Promise<DoubleTriplePredictionRow[]> {
  return fetchWithCache<DoubleTripleResponsePayload>(
    "/players/predictions/doubles",
    { day, top_n },
    PREDICTIONS_TTL
  ).then(normalizeDoubleTripleRows);
}

export function syncPlayerPropsWindow(
  params: {
    bookmakers?: string;
    markets?: string;
    min_remaining_after_call?: number;
    max_events?: number;
    schedule_mode?: "auto" | "night" | "morning" | "all";
    event_ids?: string;
  } = {}
): Promise<Record<string, unknown>> {
  return postJson<Record<string, unknown>>("/db/player-props/sync", params);
}

export function getBestBets(
  params: {
    target_multiplier?: number;
    leg_count?: number;
    leg_mode?: "exact" | "up_to";
    max_legs?: number;
    bookmaker?: string;
    day?: PredictionDayParam;
    include_combos?: boolean;
    event_ids?: string;
    min_confidence?: number;
    min_edge?: number;
    min_prob?: number;
    max_candidates?: number;
  } = {}
): Promise<BestBetsResponse> {
  return fetchWithCache<BestBetsResponse>("/bets/best", params, 2 * 60 * 1000);
}

export type UpdateJobStatus = {
  job_id: string;
  status: "queued" | "running" | "completed" | "failed" | string;
  players_done?: number;
  players_total?: number | null;
  games_done?: number;
  games_total?: number | null;
  current_game_id?: string | null;
  current_game_date?: string | null;
  type?: string;
  error?: string | null;
  result?: Record<string, unknown> | null;
};

export type LatestIngestionRunResponse = {
  status: "ok" | "empty" | string;
  latest_game_date?: string | null;
  data:
    | {
        id: number;
        ingest_type: string;
        since_date: string | null;
        season: string | null;
        status: string;
        created_at: string;
      }
    | null;
};

export type RecentPlayerGameDateRow = {
  id: number;
  player_id: number;
  game_id: string;
  game_date: string;
  matchup: string | null;
  points: number | null;
  assists: number | null;
  rebounds: number | null;
};

export function ingestGamesByDate(params: {
  since: string;
  until?: string;
  season?: string;
  include_team_stats?: boolean;
}): Promise<Record<string, unknown>> {
  return postJson<Record<string, unknown>>("/db/games/ingest", params);
}

export function startGamesIngestJob(params: {
  since: string;
  until?: string;
  season?: string;
  include_team_stats?: boolean;
}): Promise<{ status: string; job_id: string }> {
  return postJson<{ status: string; job_id: string }>("/db/games/ingest/start", params);
}

export function startLastNUpdateJob(params: {
  since: string;
  until?: string;
  season?: string;
}): Promise<{ status: string; job_id: string }> {
  return postJson<{ status: string; job_id: string }>("/db/last-n/update/start", params);
}

export function getUpdateJobStatus(jobId: string): Promise<UpdateJobStatus> {
  return fetchWithCache<UpdateJobStatus>(`/db/jobs/${jobId}`, undefined, 0);
}

export function getLatestIngestionRun(): Promise<LatestIngestionRunResponse> {
  return fetchWithCache<LatestIngestionRunResponse>("/db/ingestion-runs/latest", undefined, 10_000);
}

export function getRecentPlayerGameDates(
  limit = 5
): Promise<{ status: string; data: RecentPlayerGameDateRow[] }> {
  return fetchWithCache<{ status: string; data: RecentPlayerGameDateRow[] }>(
    "/db/player-games/recent-dates",
    { limit },
    10_000
  );
}

export function updateTeamGames(season = "2025-26"): Promise<Record<string, unknown>> {
  return postJson<Record<string, unknown>>("/db/team-games/update", { season });
}

export function refreshPlayerTeamAbbr(params: {
  season?: string;
  fallback?: boolean;
} = {}): Promise<Record<string, unknown>> {
  return postJson<Record<string, unknown>>("/db/players/refresh-team-abbr", params);
}

export function evaluateAllPredictions(): Promise<Record<string, unknown>> {
  return postJson<Record<string, unknown>>("/ml/evaluate/all");
}

export function updateRollingFeatures(): Promise<Record<string, unknown>> {
  return postJson<Record<string, unknown>>("/ml/rolling/update");
}

export function recalcUnderRiskAll(): Promise<Record<string, unknown>> {
  return postJson<Record<string, unknown>>("/db/under-risk/recalc-all");
}

export function trainAllModels(): Promise<Record<string, unknown>> {
  return postJson<Record<string, unknown>>("/ml/train/all");
}

export function runWalkforwardBacktest(
  statType: "points" | "assists" | "rebounds" | "threept" | "threepa",
  params: { reset?: boolean; min_games?: number; max_dates?: number } = {}
): Promise<Record<string, unknown>> {
  return postJson<Record<string, unknown>>(`/ml/backtest/walkforward/${statType}`, params);
}
