import { useState, useEffect, useCallback } from 'react';
import { TOKEN_LOGIN_TS_KEY, LOGIN_MAX_AGE_MS } from '../constants/defaults';
import { resolveApiBase } from '../utils/apiBase';

const useAuth = ({ setError, setHasExecutedSearch, setShowSupportPanel }) => {
  const [token, setToken] = useState('');
  const [userEmail, setUserEmail] = useState('');
  const [initialResetToken, setInitialResetToken] = useState('');
  const [pendingTosToken, setPendingTosToken] = useState('');
  const [pendingTosEmail, setPendingTosEmail] = useState('');
  const [pendingTosRedirect, setPendingTosRedirect] = useState('');

  const redirectToAuth0Login = () => {
    try {
      const base = resolveApiBase();
      const url = `${base}/login?returnTo=${encodeURIComponent(window.location.origin)}`;
      window.location.href = url;
    } catch (_) {}
  };

  const syncSessionToken = useCallback(async () => {
    try {
      const base = resolveApiBase();
      const resp = await fetch(`${base}/session/token`, { credentials: 'include' });
      if (resp.ok) {
        const data = await resp.json();
        if (data && typeof data.token === 'string') {
          setToken(data.token);
          try { localStorage.setItem('token', data.token); } catch (_) {}
          try { localStorage.setItem(TOKEN_LOGIN_TS_KEY, String(Date.now())); } catch (_) {}
          try { if (data.email) { setUserEmail(data.email); localStorage.setItem('userEmail', data.email); } } catch (_) {}
          setError('');
          return true;
        }
        return false;
      }
      // Handle ToS required case (403) to show in‑app disclaimer without Action redirects
      if (resp.status === 403) {
        let data = {};
        try { data = await resp.json(); } catch (_) {}
        const pendingTokenValue = typeof data.pendingToken === 'string' ? data.pendingToken : '';
        if (data && data.requiresTos && pendingTokenValue) {
          try { if (typeof data.email === 'string') { setUserEmail(data.email); localStorage.setItem('userEmail', data.email); } } catch (_) {}
          setPendingTosToken(pendingTokenValue);
          setPendingTosEmail(typeof data.email === 'string' ? data.email : '');
          // No page redirect; keep user in app after ToS modal
        }
      }
      return false;
    } catch (_) {
      return false;
    }
  }, []);

  const clearStoredToken = () => {
    try { localStorage.removeItem('token'); } catch (_) {}
    try { localStorage.removeItem(TOKEN_LOGIN_TS_KEY); } catch (_) {}
    try { localStorage.removeItem('userEmail'); } catch (_) {}
    setPendingTosToken('');
    setPendingTosEmail('');
    setPendingTosRedirect('');
    try { setUserEmail(''); } catch (_) {}
    try {
      const base = resolveApiBase();
      window.location.replace(`${base}/logout`);
    } catch (_) {}
  };

  // Centralized unauthorized handler: clears token and prompts re-login
  const handleUnauthorized = (resp) => {
    try {
      if (resp && (resp.status === 401 || resp.status === 403)) {
        setError('Session expired. Please log in again.');
        setToken('');
        clearStoredToken();
        setHasExecutedSearch(false);
        return true;
      }
    } catch (_) {}
    return false;
  };

  // Consume auth token handed off via URL (from backend /post-auth)
  useEffect(() => {
    try {
      const current = new URL(window.location.href);
      let tokenFrom = current.searchParams.get('authToken');
      // Also support pending ToS token delivered via URL hash or query
      let pendingFrom = current.searchParams.get('pendingTosToken');
      let pendingEmailFrom = current.searchParams.get('email');
      if (!tokenFrom) {
        const hashParams = new URLSearchParams((window.location.hash || '').replace(/^#/, ''));
        tokenFrom = hashParams.get('authToken');
        if (!pendingFrom) pendingFrom = hashParams.get('pendingTosToken');
        if (!pendingEmailFrom) pendingEmailFrom = hashParams.get('email');
        if (tokenFrom) {
          hashParams.delete('authToken');
          hashParams.delete('pendingTosToken');
          hashParams.delete('email');
          const newHash = hashParams.toString();
          const newUrl = `${current.pathname}${current.search}${newHash ? `#${newHash}` : ''}`;
          window.history.replaceState({}, '', newUrl);
        }
      } else {
        current.searchParams.delete('authToken');
        current.searchParams.delete('pendingTosToken');
        current.searchParams.delete('email');
        const newUrl = `${current.pathname}${current.search}${current.hash}`;
        window.history.replaceState({}, '', newUrl);
      }
      if (tokenFrom) {
        setToken(tokenFrom);
        try { localStorage.setItem('token', tokenFrom); } catch (_) {}
        try { localStorage.setItem(TOKEN_LOGIN_TS_KEY, String(Date.now())); } catch (_) {}
        // Derive email from the token payload so header can greet the user
        try {
          const parts = String(tokenFrom).split('.');
          if (parts.length >= 2) {
            const base64 = parts[1].replace(/-/g, '+').replace(/_/g, '/');
            const json = JSON.parse(atob(base64));
            const emailInToken = (json && typeof json.email === 'string') ? json.email.trim().toLowerCase() : '';
            if (emailInToken) {
              setUserEmail(emailInToken);
              try { localStorage.setItem('userEmail', emailInToken); } catch (_) {}
            }
          }
        } catch (_) {}
        // If asked to show support after login (from a pre-redirect flow), honor it now
        try {
          if (localStorage.getItem('cmgShowSupportAfterLogin') === '1') {
            setShowSupportPanel(true);
            localStorage.removeItem('cmgShowSupportAfterLogin');
          }
        } catch (_) {}
        setError('');
      }
      if (pendingFrom) {
        try { setPendingTosToken(pendingFrom); } catch (_) {}
        if (pendingEmailFrom) {
          try { setPendingTosEmail(pendingEmailFrom); localStorage.setItem('userEmail', pendingEmailFrom); } catch (_) {}
        }
      }
    } catch (_) {}
  }, []);

  useEffect(() => {
    if (!token) { syncSessionToken(); }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    try {
      let incomingResetToken = '';
      const params = new URLSearchParams(window.location.search);
      incomingResetToken = params.get('resetToken') || params.get('token') || '';
      if (!incomingResetToken) {
        const hash = window.location.hash || '';
        if (hash.startsWith('#')) {
          const hashParams = new URLSearchParams(hash.slice(1));
          incomingResetToken = hashParams.get('resetToken') || hashParams.get('token') || '';
        }
      }
      if (incomingResetToken) {
        setInitialResetToken(incomingResetToken);
        clearStoredToken();
        setToken('');
        setHasExecutedSearch(false);
      }
    } catch (_) {}
  }, []);

  // Initialize token on component mount
  useEffect(() => {
    let savedToken = null;
    let loginTs = null;
    try { savedToken = localStorage.getItem('token'); } catch (_) {}
    try { const rawTs = localStorage.getItem(TOKEN_LOGIN_TS_KEY); loginTs = rawTs ? parseInt(rawTs, 10) : null; } catch (_) {}
    if (!savedToken) return;
    const now = Date.now();
    const isExpired = !Number.isFinite(loginTs) || now - loginTs > LOGIN_MAX_AGE_MS;
    if (isExpired) {
      clearStoredToken();
      setError('Session expired. Please log in again.');
      setToken('');
      setHasExecutedSearch(false);
      return;
    }
    setToken(savedToken);
    try {
      const savedEmail = localStorage.getItem('userEmail');
      if (typeof savedEmail === 'string') setUserEmail(savedEmail);
    } catch (_) {}
    // If a support panel reopen was requested before a redirect, honor it now
    try {
      if (localStorage.getItem('cmgShowSupportAfterLogin') === '1') {
        setShowSupportPanel(true);
        localStorage.removeItem('cmgShowSupportAfterLogin');
      }
    } catch (_) {}
  }, []);

  return {
    token, setToken,
    userEmail, setUserEmail,
    initialResetToken, setInitialResetToken,
    pendingTosToken, setPendingTosToken,
    pendingTosEmail, setPendingTosEmail,
    pendingTosRedirect, setPendingTosRedirect,
    redirectToAuth0Login,
    syncSessionToken,
    clearStoredToken,
    handleUnauthorized,
  };
};

export default useAuth;
