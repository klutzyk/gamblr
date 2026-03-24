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

export type PredictionRow = {
  player_id: number;
  full_name: string;
  team_id?: number;
  team_abbreviation: string;
  matchup: string;
  game_date: string;
  game_id?: string;
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
  day: "today" | "tomorrow" | "yesterday" | "auto" = "auto"
): Promise<PredictionRow[]> {
  return fetchWithCache<PredictionResponsePayload>(
    "/players/predictions/points",
    { day },
    PREDICTIONS_TTL
  ).then(normalizePredictionRows);
}

export function getAssistsPredictions(
  day: "today" | "tomorrow" | "yesterday" | "auto" = "auto"
): Promise<PredictionRow[]> {
  return fetchWithCache<PredictionResponsePayload>(
    "/players/predictions/assists",
    { day },
    PREDICTIONS_TTL
  ).then(normalizePredictionRows);
}

export function getReboundsPredictions(
  day: "today" | "tomorrow" | "yesterday" | "auto" = "auto"
): Promise<PredictionRow[]> {
  return fetchWithCache<PredictionResponsePayload>(
    "/players/predictions/rebounds",
    { day },
    PREDICTIONS_TTL
  ).then(normalizePredictionRows);
}

export function getThreeptPredictions(
  day: "today" | "tomorrow" | "yesterday" | "auto" = "auto"
): Promise<PredictionRow[]> {
  return fetchWithCache<PredictionResponsePayload>(
    "/players/predictions/threept",
    { day },
    PREDICTIONS_TTL
  ).then(normalizePredictionRows);
}

export function getThreepaPredictions(
  day: "today" | "tomorrow" | "yesterday" | "auto" = "auto"
): Promise<PredictionRow[]> {
  return fetchWithCache<PredictionResponsePayload>(
    "/players/predictions/threepa",
    { day },
    PREDICTIONS_TTL
  ).then(normalizePredictionRows);
}

export function getFirstBasketPredictions(
  day: "today" | "tomorrow" | "yesterday" | "auto" = "auto",
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
  day: "today" | "tomorrow" | "yesterday" | "auto" = "auto",
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
    day?: "today" | "tomorrow" | "yesterday" | "auto";
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
