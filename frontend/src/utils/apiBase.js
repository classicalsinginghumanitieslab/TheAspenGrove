// Shared utility for resolving the API base URL.
// Checks window override, Vite env var, CRA env var, then falls back to origin.

export const resolveApiBase = () => {
  try {
    if (typeof window !== 'undefined') {
      const override = window.__CMG_API_BASE;
      if (typeof override === 'string' && override.trim()) {
        return override.trim().replace(/\/$/, '');
      }
    }
  } catch (_) {}

  let envBase = '';
  try {
    if (typeof import.meta !== 'undefined' && import.meta.env && typeof import.meta.env.VITE_API_BASE === 'string') {
      envBase = import.meta.env.VITE_API_BASE;
    } else if (typeof process !== 'undefined' && process?.env?.REACT_APP_API_BASE) {
      envBase = process.env.REACT_APP_API_BASE;
    }
  } catch (_) {}

  envBase = (envBase || '').trim();
  if (envBase) return envBase.replace(/\/$/, '');

  if (typeof window !== 'undefined') {
    const { protocol, hostname } = window.location;
    if (hostname === 'localhost' || hostname === '127.0.0.1') return 'http://localhost:3001';
    return `${protocol}//${hostname}`;
  }

  return 'http://localhost:3001';
};
