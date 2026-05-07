import { useState, useRef, useEffect } from 'react';
import { RATE_LIMIT_MIN_WAIT_MS, RATE_LIMIT_DEFAULT_WAIT_MS } from '../constants/defaults';

const useRateLimit = ({ setError }) => {
  const [rateLimitedUntil, setRateLimitedUntil] = useState(0);
  const rateLimitedUntilRef = useRef(0);
  const rateLimitClearTimeoutRef = useRef(null);
  const rateLimitIntervalRef = useRef(null);
  const rateLimitMessageTokenRef = useRef(0);
  const rateLimitCooldownTimeoutRef = useRef(null);

  const sleep = (ms) => new Promise(res => setTimeout(res, ms));

  const isRateLimitMessage = (msg) => typeof msg === 'string' && msg.startsWith('Too many requests –');
  const formatRateLimitWaitMessage = (untilTs) => {
    if (!untilTs) return 'Too many requests – please try again shortly.';
    const msRemaining = Math.max(0, untilTs - Date.now());
    const secs = Math.max(1, Math.ceil(msRemaining / 1000));
    return `Too many requests – please try again in ${secs}s`;
  };
  const scheduleRateLimitCooldown = (rawWaitMs = RATE_LIMIT_DEFAULT_WAIT_MS, { suppressMessage = false } = {}) => {
    const waitMs = Math.max(RATE_LIMIT_MIN_WAIT_MS, rawWaitMs || RATE_LIMIT_DEFAULT_WAIT_MS);
    const nowTs = Date.now();
    const currentUntil = rateLimitedUntilRef.current || 0;
    const proposedUntil = nowTs + waitMs;
    const untilTs = Math.max(currentUntil, proposedUntil);
    rateLimitedUntilRef.current = untilTs;
    try { setRateLimitedUntil(untilTs); } catch (_) {}
    if (rateLimitCooldownTimeoutRef.current) {
      clearTimeout(rateLimitCooldownTimeoutRef.current);
      rateLimitCooldownTimeoutRef.current = null;
    }
    const cooldownWait = Math.max(0, untilTs - nowTs);
    rateLimitCooldownTimeoutRef.current = setTimeout(() => {
      if (rateLimitedUntilRef.current <= untilTs) {
        rateLimitedUntilRef.current = 0;
        try { setRateLimitedUntil(0); } catch (_) {}
      }
    }, cooldownWait + 50);
    if (suppressMessage) {
      return { waitMs, untilTs, message: formatRateLimitWaitMessage(untilTs) };
    }
    if (rateLimitClearTimeoutRef.current) {
      clearTimeout(rateLimitClearTimeoutRef.current);
      rateLimitClearTimeoutRef.current = null;
    }
    if (rateLimitIntervalRef.current) {
      clearInterval(rateLimitIntervalRef.current);
      rateLimitIntervalRef.current = null;
    }
    const token = (rateLimitMessageTokenRef.current || 0) + 1;
    rateLimitMessageTokenRef.current = token;
    const updateCountdownMessage = () => {
      if (rateLimitMessageTokenRef.current !== token) return;
      const currentUntil = rateLimitedUntilRef.current || 0;
      if (!currentUntil || Date.now() >= currentUntil) {
        rateLimitedUntilRef.current = 0;
        try { setRateLimitedUntil(0); } catch (_) {}
        try {
          setError((prev) => (isRateLimitMessage(prev) ? '' : prev));
        } catch (_) {}
        if (rateLimitIntervalRef.current) {
          clearInterval(rateLimitIntervalRef.current);
          rateLimitIntervalRef.current = null;
        }
        return;
      }
      try { setError(formatRateLimitWaitMessage(currentUntil)); } catch (_) {}
    };
    updateCountdownMessage();
    rateLimitIntervalRef.current = setInterval(updateCountdownMessage, 1000);
    rateLimitClearTimeoutRef.current = setTimeout(() => {
      if (rateLimitMessageTokenRef.current !== token) return;
      rateLimitClearTimeoutRef.current = null;
      updateCountdownMessage();
    }, cooldownWait + 150);
    return { waitMs, untilTs, message: formatRateLimitWaitMessage(untilTs) };
  };
  const handleRateLimitResponse = (response, fallbackMs = RATE_LIMIT_DEFAULT_WAIT_MS, options = {}) => {
    if (!response || response.status !== 429) return null;
    const headers = response.headers && typeof response.headers.get === 'function' ? response.headers : null;
    const retryAfterHeader = headers ? (headers.get('Retry-After') || headers.get('retry-after')) : null;
    const rateLimitResetHeader = headers ? (headers.get('RateLimit-Reset') || headers.get('ratelimit-reset')) : null;
    let waitMs = fallbackMs;
    if (retryAfterHeader) {
      const parsed = Number(retryAfterHeader);
      if (Number.isFinite(parsed) && parsed >= 0) {
        waitMs = parsed * 1000;
      }
    } else if (rateLimitResetHeader) {
      const parsed = Number(rateLimitResetHeader);
      if (Number.isFinite(parsed) && parsed >= 0) {
        waitMs = Math.max(waitMs, parsed * 1000);
      }
    }
    return scheduleRateLimitCooldown(waitMs, options);
  };
  useEffect(() => {
    return () => {
      if (rateLimitClearTimeoutRef.current) {
        clearTimeout(rateLimitClearTimeoutRef.current);
        rateLimitClearTimeoutRef.current = null;
      }
      if (rateLimitIntervalRef.current) {
        clearInterval(rateLimitIntervalRef.current);
        rateLimitIntervalRef.current = null;
      }
      if (rateLimitCooldownTimeoutRef.current) {
        clearTimeout(rateLimitCooldownTimeoutRef.current);
        rateLimitCooldownTimeoutRef.current = null;
      }
    };
  }, []);

  // Fetch with retry and exponential backoff (handles 429 with Retry-After)
  const fetchWithRetry = async (url, options = {}, { retries = 2, baseDelay = 500 } = {}) => {
    let attempt = 0;
    let lastErr;
    let lastStatus = null;
    let lastRateLimitMessage = '';
    while (attempt <= retries) {
      try {
        // Global cooldown if we've recently been rate-limited
        const now = Date.now();
        const until = rateLimitedUntilRef.current || 0;
        if (until && now < until) {
          await sleep(Math.min(until - now, 10000));
        }
        const resp = await fetch(url, options);
        lastStatus = resp.status;
        if (resp.status === 429) {
          const fallbackDelay = baseDelay * Math.pow(2, attempt);
          // Suppress the countdown banner during auto-retry (2026-05-06): if
          // the next attempt succeeds the user shouldn't see a misleading
          // "Too many requests" pill for an action that completed. The
          // cooldown ref is still set so subsequent user actions will be
          // fail-fast'd by checkAndEnforceRateLimit and THAT will surface
          // the banner. If retries here exhaust, we surface it below.
          const info = handleRateLimitResponse(resp, fallbackDelay, { suppressMessage: true });
          lastRateLimitMessage = info?.message || formatRateLimitWaitMessage(rateLimitedUntilRef.current);
          await sleep(info?.waitMs || fallbackDelay || RATE_LIMIT_DEFAULT_WAIT_MS);
          attempt += 1;
          continue;
        }
        return resp;
      } catch (e) {
        lastErr = e;
        await sleep(baseDelay * Math.pow(2, attempt));
        attempt += 1;
      }
    }
    if (lastErr) throw lastErr;
    if (lastStatus === 429) {
      // Retries exhausted on a 429 — now surface the live countdown banner
      // since the user's action genuinely failed. Pass the remaining time
      // so we don't extend the cooldown beyond what the backend dictated.
      const remaining = Math.max(0, (rateLimitedUntilRef.current || 0) - Date.now());
      scheduleRateLimitCooldown(remaining || RATE_LIMIT_DEFAULT_WAIT_MS);
      throw new Error(lastRateLimitMessage || formatRateLimitWaitMessage(rateLimitedUntilRef.current || (Date.now() + RATE_LIMIT_DEFAULT_WAIT_MS)));
    }
    throw new Error('Request failed');
  };

  return {
    rateLimitedUntil,
    rateLimitedUntilRef,
    scheduleRateLimitCooldown,
    handleRateLimitResponse,
    isRateLimitMessage,
    formatRateLimitWaitMessage,
    sleep,
    fetchWithRetry,
  };
};

export default useRateLimit;
