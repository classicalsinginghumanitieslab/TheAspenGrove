// Application-wide default values and localStorage key names.

// --- localStorage / sessionStorage keys ---

export const SESSION_SNAPSHOT_KEY = 'cmgActiveSession_v1';
export const SESSION_SNAPSHOT_FILTERLESS_KEY = 'cmgActiveSession_filtersReset';
export const TOKEN_LOGIN_TS_KEY = 'cmgTokenLoginTs';

// How long a token-based login remains valid (14 days in ms).
export const LOGIN_MAX_AGE_MS = 14 * 24 * 60 * 60 * 1000;

// --- Filter defaults ---

// Inclusive year range shown by the birth-year slider on first load.
export const DEFAULT_BIRTH_RANGE = [1534, 2005];

// Inclusive year range shown by the death-year slider on first load.
export const DEFAULT_DEATH_RANGE = [1575, 2025];

// --- Rate-limit handling ---

// Minimum cooldown duration after a 429 response (ms).
export const RATE_LIMIT_MIN_WAIT_MS = 1000;

// Default cooldown duration when no Retry-After header is present (ms).
export const RATE_LIMIT_DEFAULT_WAIT_MS = 2000;
