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
  model_version?: string;
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

// Predictions endpoint (cached for 5 minutes since predictions may update)
const PREDICTIONS_TTL = 5 * 60 * 1000;

export function getPointsPredictions(
  day: "today" | "tomorrow" | "yesterday" | "auto" = "auto"
): Promise<PredictionRow[]> {
  return fetchWithCache<PredictionRow[]>(
    "/players/predictions/points",
    { day },
    PREDICTIONS_TTL
  );
}

export function getAssistsPredictions(
  day: "today" | "tomorrow" | "yesterday" | "auto" = "auto"
): Promise<PredictionRow[]> {
  return fetchWithCache<PredictionRow[]>(
    "/players/predictions/assists",
    { day },
    PREDICTIONS_TTL
  );
}

export function getReboundsPredictions(
  day: "today" | "tomorrow" | "yesterday" | "auto" = "auto"
): Promise<PredictionRow[]> {
  return fetchWithCache<PredictionRow[]>(
    "/players/predictions/rebounds",
    { day },
    PREDICTIONS_TTL
  );
}
