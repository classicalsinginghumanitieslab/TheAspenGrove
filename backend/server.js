import dotenv from 'dotenv';
dotenv.config();
import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import rateLimit from 'express-rate-limit';
import neo4j from 'neo4j-driver';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import crypto from 'crypto';
import mysql from 'mysql2/promise';
import sgMail from '@sendgrid/mail';
import { auth as auth0Middleware } from 'express-openid-connect';
import createAuth0MigrationRouter from './routes/auth0Migration.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3001;
const HOST = process.env.HOST || '0.0.0.0';
const JWT_SECRET = process.env.JWT_SECRET || 'dev-secret';
const JWT_EXPIRES_IN = process.env.JWT_EXPIRES_IN || '7d';
const MYSQL_URL = process.env.MYSQL_URL || process.env.DATABASE_URL || process.env.MYSQL_CONNECTION_URL || '';
const MYSQL_HOST = process.env.MYSQL_HOST;
const MYSQL_PORT = process.env.MYSQL_PORT;
const MYSQL_USER = process.env.MYSQL_USER;
const MYSQL_PASSWORD = process.env.MYSQL_PASSWORD;
const MYSQL_DATABASE = process.env.MYSQL_DATABASE;
const MYSQL_SSL = String(process.env.MYSQL_SSL || '').toLowerCase();
const MYSQL_CONNECTION_LIMIT = parseInt(process.env.MYSQL_CONNECTION_LIMIT || '10', 10);
const SENDGRID_API_KEY = process.env.SENDGRID_API_KEY || '';
const SENDGRID_FROM_EMAIL = process.env.SENDGRID_FROM_EMAIL || '';
const RESET_URL_BASE = (process.env.RESET_URL_BASE || '').trim();
const CLIENT_BASE_URL = (process.env.CLIENT_BASE_URL || '').trim();
const PASSWORD_RESET_TOKEN_TTL_MINUTES = parseInt(process.env.PASSWORD_RESET_TOKEN_TTL_MINUTES || '60', 10);
const PASSWORD_RESET_TOKEN_BYTES = Math.max(parseInt(process.env.PASSWORD_RESET_TOKEN_BYTES || '32', 10), 16);
const MAX_SNAPSHOT_BYTES = parseInt(process.env.MAX_SNAPSHOT_BYTES || '1500000', 10); // ~1.5 MB
const ACTIVE_TOS_VERSION = parseInt(process.env.ACTIVE_TOS_VERSION || '1', 10);
// Global DB write guard (stops writing to Railway when disabled)
const DB_WRITES_ENABLED = !/^(0|false|no|off)$/i.test(String(process.env.DB_WRITE_ENABLED || '1'));
const MYSQL_ALLOW_USER_CREATION = /^(1|true|yes|on)$/i.test(String(process.env.MYSQL_ALLOW_USER_CREATION || '0'));
// Local password reset feature flag (off = rely on Auth0)
const PASSWORD_RESET_ENABLED = /^(1|true|yes|on)$/i.test(String(process.env.PASSWORD_RESET_ENABLED || '0'));
// Enable internal /debug/* endpoints only when explicitly allowed
const DEBUG_ENDPOINTS_ENABLED = /^(1|true|yes|on)$/i.test(String(process.env.DEBUG_ENDPOINTS_ENABLED || '0'));
// ToS enforcement mode:
//   'legacy'     -> enforce using local MySQL (default), but trust Auth0 app_metadata to auto-accept.
//   'auth0_only' -> enforce using Auth0 app_metadata only; block if missing/outdated.
//   'off'        -> do not block on ToS (useful during migration/testing).
const TOS_ENFORCEMENT_MODE = (process.env.TOS_ENFORCEMENT_MODE || 'legacy').toLowerCase();
const AUTH0_MIGRATION_TOKEN = process.env.AUTH0_MIGRATION_TOKEN || '';
const APP_SESSION_SECRET = (process.env.APP_SESSION_SECRET || '').trim();
const BASE_URL = ((process.env.BASE_URL || '').trim() || `http://localhost:${PORT}`).replace(/\/$/, '');
const AUTH0_ISSUER_BASE_URL = (process.env.AUTH0_ISSUER_BASE_URL || '').trim().replace(/\/$/, '');
const AUTH0_CLIENT_ID = (process.env.AUTH0_CLIENT_ID || '').trim();
const AUTH0_CLIENT_SECRET = (process.env.AUTH0_CLIENT_SECRET || '').trim();
const AUTH0_AUDIENCE = (process.env.AUTH0_AUDIENCE || '').trim();
const AUTH0_SCOPE = (process.env.AUTH0_SCOPE || 'openid profile email').trim();
const AUTH0_CONNECTION = (process.env.AUTH0_CONNECTION || '').trim();
const AUTH0_COOKIE_DOMAIN = (process.env.AUTH0_COOKIE_DOMAIN || '').trim();
// Optional namespace for custom Auth0 ID token claims (used to carry ToS info)
const AUTH0_CLAIM_NAMESPACE = ((process.env.AUTH0_CLAIM_NAMESPACE || 'https://theaspengrove.org').trim() || 'https://theaspengrove.org').replace(/\/$/, '');
const AUTH0_TOS_CLAIM = `${AUTH0_CLAIM_NAMESPACE}/tos_version`;
const AUTH0_TOS_ACCEPTED_AT_CLAIM = `${AUTH0_CLAIM_NAMESPACE}/tos_accepted_at`;
// Avoid writing ToS into MySQL users table by default
const MYSQL_SYNC_TOS_TO_USERS = /^(1|true|yes|on)$/i.test(String(process.env.MYSQL_SYNC_TOS_TO_USERS || '0'));
// Auth0 Management API (optional, enables server-side ToS persistence)
const AUTH0_MGMT_CLIENT_ID = (process.env.AUTH0_MGMT_CLIENT_ID || '').trim();
const AUTH0_MGMT_CLIENT_SECRET = (process.env.AUTH0_MGMT_CLIENT_SECRET || '').trim();
const AUTH0_MGMT_AUDIENCE = `${AUTH0_ISSUER_BASE_URL}/api/v2/`;
const AUTH0_BASE_IS_HTTPS = /^https:\/\//i.test(BASE_URL);
const AUTH0_SESSION_COOKIE = {
  sameSite: AUTH0_BASE_IS_HTTPS ? 'None' : 'Lax',
  secure: AUTH0_BASE_IS_HTTPS,
  httpOnly: true
};
if (AUTH0_COOKIE_DOMAIN) {
  AUTH0_SESSION_COOKIE.domain = AUTH0_COOKIE_DOMAIN;
}
const AUTH0_CONFIG_COMPLETE = Boolean(
  APP_SESSION_SECRET &&
  BASE_URL &&
  AUTH0_ISSUER_BASE_URL &&
  AUTH0_CLIENT_ID
);
  const auth0Config = AUTH0_CONFIG_COMPLETE
  ? {
      authRequired: false,
      auth0Logout: true,
      secret: APP_SESSION_SECRET,
      baseURL: BASE_URL,
      clientID: AUTH0_CLIENT_ID,
      issuerBaseURL: AUTH0_ISSUER_BASE_URL,
      authorizationParams: {
        response_type: 'code',
        scope: AUTH0_SCOPE,
        ...(AUTH0_AUDIENCE ? { audience: AUTH0_AUDIENCE } : {})
      },
      routes: {
        login: false,
        logout: '/logout',
        callback: '/callback',
        postLogoutRedirect: (CLIENT_BASE_URL || BASE_URL)
      },
      session: {
        rolling: true,
        rollingDuration: 24 * 60 * 60,
        absoluteDuration: 7 * 24 * 60 * 60,
        cookie: AUTH0_SESSION_COOKIE
      }
    }
  : null;
if (auth0Config && AUTH0_CLIENT_SECRET) {
  auth0Config.clientSecret = AUTH0_CLIENT_SECRET;
}
console.log('🔑 AUTH0_MIGRATION_TOKEN:', AUTH0_MIGRATION_TOKEN ? 'LOADED ✓' : 'NOT LOADED ✗');

const EFFECTIVE_RESET_URL_BASE = (RESET_URL_BASE || CLIENT_BASE_URL || '').replace(/\/$/, '');

if (SENDGRID_API_KEY) {
  try {
    sgMail.setApiKey(SENDGRID_API_KEY);
  } catch (err) {
    console.error('[SendGrid] Failed to set API key:', err?.message || err);
  }
} else {
  console.warn('[SendGrid] SENDGRID_API_KEY is not set. Password reset emails will be disabled.');
}

if (!SENDGRID_FROM_EMAIL) {
  console.warn('[SendGrid] SENDGRID_FROM_EMAIL is not set. Password reset emails will be disabled.');
}

if (!EFFECTIVE_RESET_URL_BASE) {
  console.warn('[Password Reset] RESET_URL_BASE or CLIENT_BASE_URL is not set. Reset links cannot be generated.');
}

const isPasswordResetEmailConfigured = () =>
  Boolean(SENDGRID_API_KEY && SENDGRID_FROM_EMAIL && EFFECTIVE_RESET_URL_BASE);

const generateAuthToken = (email) =>
  jwt.sign({ email, tosVersion: ACTIVE_TOS_VERSION }, JWT_SECRET, { expiresIn: JWT_EXPIRES_IN });

const generatePendingTosToken = (email) =>
  jwt.sign({ email, purpose: 'tos_accept', tosVersion: ACTIVE_TOS_VERSION }, JWT_SECRET, { expiresIn: '15m' });

let mysqlPoolPromise = null;
const getMysqlPool = () => {
  if (!mysqlPoolPromise) {
    mysqlPoolPromise = (async () => {
      let config;
      if (MYSQL_URL) {
        const url = new URL(MYSQL_URL);
        config = {
          host: url.hostname,
          port: url.port ? Number(url.port) : 3306,
          user: decodeURIComponent(url.username),
          password: decodeURIComponent(url.password),
          database: url.pathname.replace(/^\//, '') || undefined
        };
      } else {
        config = {
          host: MYSQL_HOST || 'localhost',
          port: MYSQL_PORT ? Number(MYSQL_PORT) : 3306,
          user: MYSQL_USER || 'root',
          password: MYSQL_PASSWORD || '',
          database: MYSQL_DATABASE || 'cmg_auth'
        };
      }
      if (MYSQL_SSL === 'true' || MYSQL_SSL === '1') {
        config.ssl = { rejectUnauthorized: false };
      }
      const pool = mysql.createPool({
        waitForConnections: true,
        connectionLimit: MYSQL_CONNECTION_LIMIT,
        maxIdle: Math.min(MYSQL_CONNECTION_LIMIT, 2),
        idleTimeout: 60000,
        enableKeepAlive: true,
        keepAliveInitialDelay: 0,
        ...config
      });
      if (DB_WRITES_ENABLED) {
        await pool.query(`
          CREATE TABLE IF NOT EXISTS users (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            email VARCHAR(320) NOT NULL UNIQUE,
            password_hash VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )
          ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        `);
        const ensureColumnExists = async (column, definition) => {
          const [columns] = await pool.query(
            `SHOW COLUMNS FROM users LIKE ?`,
            [column]
          );
          if (columns.length === 0) {
            await pool.query(`ALTER TABLE users ADD COLUMN ${definition}`);
          }
        };
        await ensureColumnExists('tos_accepted_at', 'tos_accepted_at DATETIME NULL DEFAULT NULL');
        await ensureColumnExists('tos_version', 'tos_version TINYINT UNSIGNED NOT NULL DEFAULT 0');
        await ensureColumnExists('public_user_id', 'public_user_id CHAR(12) NULL UNIQUE');
        await pool.query(`
          CREATE TABLE IF NOT EXISTS saved_views (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            user_public_id CHAR(12) NOT NULL,
            token CHAR(36) NOT NULL UNIQUE,
            label VARCHAR(255) DEFAULT '',
            snapshot JSON NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_saved_views_user_public (user_public_id)
          )
          ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        `);
      }
      if (PASSWORD_RESET_ENABLED) {
        await pool.query(`
          CREATE TABLE IF NOT EXISTS password_resets (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            user_email VARCHAR(320) NOT NULL,
            token_hash CHAR(64) NOT NULL UNIQUE,
            expires_at DATETIME NOT NULL,
            consumed_at DATETIME DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_password_resets_email (user_email),
            INDEX idx_password_resets_expires (expires_at)
          )
          ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        `);
      }
      return pool;
    })();
  }
  return mysqlPoolPromise;
};

// Ensure the saved_views table exists with the current minimal schema (id, user_public_id, token, label, snapshot, created_at)
const ensureSavedViewsTable = async (pool) => {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS saved_views (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        user_public_id CHAR(12) NOT NULL,
        token CHAR(36) NOT NULL UNIQUE,
        label VARCHAR(255) DEFAULT '',
        snapshot JSON NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_saved_views_user_public (user_public_id)
      )
      ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    `);
  } catch (e) {
    // Let callers handle failures
    throw e;
  }
};

// Generate a short random public ID (8 chars [A-Z0-9])
const generatePublicId = () => {
  const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // no 0/O/I/L for readability
  const bytes = crypto.randomBytes(8);
  let out = '';
  for (let i = 0; i < bytes.length; i += 1) out += alphabet[bytes[i] % alphabet.length];
  return out;
};

const ensureUserRecordForEmail = async (email) => {
  const normalized = (email || '').trim().toLowerCase();
  if (!normalized) return null;
  const pool = await getMysqlPool();
  let rows;
  try {
    [rows] = await pool.query(
      'SELECT id, email, tos_version, tos_accepted_at, public_user_id FROM users WHERE email = ? LIMIT 1',
      [normalized]
    );
  } catch (e) {
    // public_user_id column may not exist yet; fall back to legacy shape
    [rows] = await pool.query(
      'SELECT id, email, tos_version, tos_accepted_at FROM users WHERE email = ? LIMIT 1',
      [normalized]
    );
  }
  if (rows.length > 0) {
    return rows[0];
  }
  // Do not create new rows unless explicitly allowed
  if (!DB_WRITES_ENABLED || !MYSQL_ALLOW_USER_CREATION) {
    return { id: 0, email: normalized, tos_version: 0, tos_accepted_at: null, public_user_id: null };
  }
  try {
    const placeholderPassword = await bcrypt.hash(crypto.randomBytes(32).toString('hex'), 10);
    await pool.query(
      `INSERT IGNORE INTO users (email, password_hash, tos_version, tos_accepted_at)
       VALUES (?, ?, 0, NULL)`,
      [normalized, placeholderPassword]
    );
    const [reloaded] = await pool.query(
      'SELECT id, email, tos_version, tos_accepted_at, public_user_id FROM users WHERE email = ? LIMIT 1',
      [normalized]
    );
    return reloaded.length > 0 ? reloaded[0] : null;
  } catch (_) {
    return { id: 0, email: normalized, tos_version: 0, tos_accepted_at: null, public_user_id: null };
  }
};

// Ensure a stable public_user_id exists, persisted in MySQL when writable, or in Auth0 app_metadata via M2M
const getOrCreatePublicIdForEmail = async (email) => {
  const normalized = (email || '').trim().toLowerCase();
  if (!normalized) return null;
  try {
    const rec = await ensureUserRecordForEmail(normalized);
    if (rec && rec.public_user_id) {
      // Mirror to Auth0 app_metadata if missing or different
      if (AUTH0_MGMT_CLIENT_ID && AUTH0_MGMT_CLIENT_SECRET) {
        try {
          const u = await mgmtFindUserByEmail(normalized);
          const existing = u && u.app_metadata ? u.app_metadata.public_user_id : null;
          if (u && u.user_id && (!existing || existing !== rec.public_user_id)) {
            await mgmtUpdateUserAppMetadata(u.user_id, { public_user_id: rec.public_user_id });
          }
        } catch (_) {}
      }
      return rec.public_user_id;
    }
  } catch (_) {}
  // Try Auth0 app_metadata via Management API
  let mgmtUser = null;
  if (AUTH0_MGMT_CLIENT_ID && AUTH0_MGMT_CLIENT_SECRET) {
    try { mgmtUser = await mgmtFindUserByEmail(normalized); } catch (_) { mgmtUser = null; }
    const existing = mgmtUser && mgmtUser.app_metadata && mgmtUser.app_metadata.public_user_id;
    if (existing) return existing;
  }
  // Generate a new one and persist to Auth0 only (no MySQL writes to users)
  const newId = generatePublicId();
  if (AUTH0_MGMT_CLIENT_ID && AUTH0_MGMT_CLIENT_SECRET) {
    try {
      if (!mgmtUser) mgmtUser = await mgmtFindUserByEmail(normalized);
      if (mgmtUser && mgmtUser.user_id) {
        await mgmtUpdateUserAppMetadata(mgmtUser.user_id, { public_user_id: newId });
        return newId;
      }
    } catch (_) {}
  }
  return newId;
};

const buildResetLink = (token) => {
  if (!EFFECTIVE_RESET_URL_BASE) {
    throw new Error('RESET_URL_BASE (or CLIENT_BASE_URL) is not configured.');
  }
  const base = EFFECTIVE_RESET_URL_BASE.replace(/\/$/, '');
  const encoded = encodeURIComponent(token);
  return `${base}/reset-password?token=${encoded}#resetToken=${encoded}`;
};

const sendPasswordResetEmail = async (recipientEmail, token) => {
  if (!isPasswordResetEmailConfigured()) {
    throw new Error('Password reset email configuration is incomplete.');
  }
  const resetUrl = buildResetLink(token);
  const msg = {
    to: recipientEmail,
    from: SENDGRID_FROM_EMAIL,
    subject: 'Reset your Aspen Grove password',
    text: [
      'You recently requested to reset your password for The Aspen Grove of Opera Singers.',
      'Use the secure link below to choose a new password. This link expires in',
      `${PASSWORD_RESET_TOKEN_TTL_MINUTES} minutes.`,
      '',
      resetUrl,
      '',
      `Reset code: ${token}`,
      '',
      'If you did not request this change, you can safely ignore this email.'
    ].join('\n'),
    html: `
      <p>You recently requested to reset your password for <strong>The Aspen Grove of Opera Singers</strong>.</p>
      <p>Use the secure link below to choose a new password. This link expires in ${PASSWORD_RESET_TOKEN_TTL_MINUTES} minutes.</p>
      <p><a href="${resetUrl}">${resetUrl}</a></p>
      <p><strong>Reset code:</strong> ${token}</p>
      <p>If you did not request this change, you can safely ignore this email.</p>
    `
  };
  await sgMail.send(msg);
};

const createPasswordResetRecord = async (pool, userEmail) => {
  const rawToken = crypto.randomBytes(PASSWORD_RESET_TOKEN_BYTES).toString('hex');
  const tokenHash = crypto.createHash('sha256').update(rawToken).digest('hex');
  const expiresAt = new Date(Date.now() + PASSWORD_RESET_TOKEN_TTL_MINUTES * 60 * 1000);
  await pool.query(
    'INSERT INTO password_resets (user_email, token_hash, expires_at) VALUES (?, ?, ?)',
    [userEmail, tokenHash, expiresAt]
  );
  return { rawToken, expiresAt };
};

// Security middleware
app.set('trust proxy', 1);
app.use(helmet());
// Simple request timing logger (path, status, duration)
app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => {
    const ms = Date.now() - start;
    try {
      console.log(`[req] ${req.method} ${req.originalUrl} -> ${res.statusCode} in ${ms}ms`);
    } catch (_) {}
  });
  next();
});
const rawClientOrigins = process.env.CLIENT_ORIGINS;
console.log('[CORS] raw CLIENT_ORIGINS:', rawClientOrigins);
const normalizeOrigin = (origin = '') => origin.trim().replace(/\/$/, '');
const allowedOrigins =
  (rawClientOrigins &&
    rawClientOrigins
      .split(',')
      .map((origin) => normalizeOrigin(origin))
      .filter(Boolean)) || [
    'http://localhost:3000',
    'http://localhost:3002',
    'http://localhost:5173',
    'https://aspengrove.netlify.app'
  ];
const allowedOriginsSet = new Set(allowedOrigins.map(normalizeOrigin));
console.log('[CORS] Allowed origins:', Array.from(allowedOriginsSet));

const ensureOriginTracked = (set, origin) => {
  const normalized = normalizeOrigin(origin);
  if (normalized) set.add(normalized);
};
const allowedReturnToOrigins = new Set(allowedOriginsSet);
ensureOriginTracked(allowedReturnToOrigins, CLIENT_BASE_URL);
ensureOriginTracked(allowedReturnToOrigins, BASE_URL);
const DEFAULT_RETURN_TO =
  CLIENT_BASE_URL ||
  (allowedOrigins.length > 0 ? allowedOrigins[0] : null) ||
  BASE_URL;
const resolveAllowedReturnTo = (candidate) => {
  if (typeof candidate === 'string' && candidate.trim()) {
    try {
      const parsed = new URL(candidate.trim());
      const normalizedOrigin = normalizeOrigin(`${parsed.protocol}//${parsed.host}`);
      if (allowedReturnToOrigins.has(normalizedOrigin)) {
        return `${normalizedOrigin}${parsed.pathname}${parsed.search}${parsed.hash}`;
      }
    } catch (_) {}
  }
  return DEFAULT_RETURN_TO;
};

const corsOptions = {
  origin(origin, cb) {
    if (!origin) return cb(null, true); // allow non-browser clients / curl
    const normalized = normalizeOrigin(origin);
    const ok =
      allowedOriginsSet.has(normalized) ||
      privateLan.some((rx) => rx.test(origin)) ||
      cloudflareTunnel.test(normalized);
    if (ok) {
      return cb(null, true);
    }
    console.warn(`[CORS] Blocked origin: ${origin} (normalized: ${normalized})`);
    return cb(new Error(`Not allowed by CORS: ${origin}`));
  },
  credentials: true,
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  exposedHeaders: ['Retry-After', 'RateLimit-Reset', 'RateLimit-Remaining', 'RateLimit-Limit']
};

app.use(cors(corsOptions));
app.options('*', cors(corsOptions));

// Rate limiting
let rateLimitHits = 0;
setInterval(() => {
  if (rateLimitHits > 0) {
    console.log(`[ratelimit] hits in last 60s: ${rateLimitHits}`);
    rateLimitHits = 0;
  }
}, 60 * 1000);
const buildLimiter = (maxPerMinute) => rateLimit({
  windowMs: 60 * 1000,
  max: maxPerMinute,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res, _next, options) => {
    rateLimitHits += 1;
    const retryAfterSeconds = Math.ceil(options.windowMs / 1000);
    res.set('Retry-After', retryAfterSeconds.toString());
    return res.status(options.statusCode).json({
      error: 'Too many requests. Please wait before retrying.',
      retryAfterSeconds
    });
  }
});
// Gentle global limiter as a safety net
app.use(buildLimiter(300));

// Route-specific limiters
const authLimiter = buildLimiter(60);
const networkLimiter = buildLimiter(30); // heavier endpoints
const detailsLimiter = buildLimiter(60);
const viewsReadLimiter = buildLimiter(60);
const viewsWriteLimiter = buildLimiter(20);

app.use(express.json({ limit: '10mb' }));

if (auth0Config) {
  app.use(auth0Middleware(auth0Config));
  console.log('[Auth0] Session bridge enabled. Base URL:', BASE_URL);

  app.get('/login', (req, res) => {
    if (!req.oidc || !res.oidc) {
      return res.status(500).json({ error: 'Auth0 session context unavailable.' });
    }
    const fromQuery =
      (typeof req.query?.returnTo === 'string' && req.query.returnTo) ||
      (typeof req.query?.redirect === 'string' && req.query.redirect) ||
      (typeof req.query?.target === 'string' && req.query.target) ||
      '';
    const desiredFrontUrl = resolveAllowedReturnTo(fromQuery);
    // Bounce back through backend to stamp an app token in the URL hash, avoiding third‑party cookie issues.
    const postAuthUrl = `${BASE_URL}/post-auth?next=${encodeURIComponent(desiredFrontUrl)}`;
    const options = { returnTo: postAuthUrl };
    if (AUTH0_CONNECTION) {
      options.authorizationParams = { connection: AUTH0_CONNECTION };
    }
    return res.oidc.login(options);
  });
} else {
  console.warn('[Auth0] Session bridge disabled. Missing APP_SESSION_SECRET, BASE_URL, AUTH0_CLIENT_ID, or AUTH0_ISSUER_BASE_URL.');
}

// Mount Auth0 migration router
app.use(createAuth0MigrationRouter({ 
  getMysqlPool, 
  expectedToken: AUTH0_MIGRATION_TOKEN,
  activeTosVersion: ACTIVE_TOS_VERSION,
  dbWritesEnabled: DB_WRITES_ENABLED
}));

// Neo4j connection
const NEO4J_URI = (process.env.NEO4J_URI || 'bolt://localhost:7687').trim();
const NEO4J_USER = process.env.NEO4J_USER || process.env.NEO4J_USERNAME || 'neo4j';
const NEO4J_PASSWORD = process.env.NEO4J_PASSWORD || process.env.NEO4J_PASS || 'password';
const NEO4J_ENCRYPTION = (process.env.NEO4J_ENCRYPTION || 'off').toLowerCase();

// If URI already encodes encryption (neo4j+s, bolt+s, neo4j+ssc, bolt+ssc),
// do NOT pass encryption in config or driver will throw.
const auth = neo4j.auth.basic(NEO4J_USER, NEO4J_PASSWORD);
const uriHasEncryptionScheme = /^(bolt|neo4j)\+/.test(NEO4J_URI);
const driver = uriHasEncryptionScheme
  ? neo4j.driver(NEO4J_URI, auth)
  : neo4j.driver(
      NEO4J_URI,
      auth,
      { encrypted: NEO4J_ENCRYPTION === 'on' ? 'ENCRYPTION_ON' : 'ENCRYPTION_OFF' }
    );

const parseSearchLimit = (value) => {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return Math.min(parsed, 1000);
};

const toPlainNumber = (value, defaultValue = 0) => {
  if (value === null || value === undefined) return defaultValue;
  if (typeof neo4j.isInt === 'function' && neo4j.isInt(value)) {
    try {
      return value.toNumber();
    } catch (_) {
      return defaultValue;
    }
  }
  if (typeof value === 'number') return value;
  const num = Number(value);
  return Number.isFinite(num) ? num : defaultValue;
};

const toPlainString = (value) => {
  if (value === null || value === undefined) return null;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed || null;
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }
  if (typeof value === 'object') {
    try {
      const text = value.toString();
      if (typeof text === 'string') {
        const trimmed = text.trim();
        return trimmed || null;
      }
    } catch (_) {
      return null;
    }
  }
  try {
    const text = String(value).trim();
    return text || null;
  } catch (_) {
    return null;
  }
};

const normalizeIdComponent = (value) => {
  if (value === null || value === undefined) return '';
  return String(value).trim();
};

const resolvePersonId = (props = {}) =>
  normalizeIdComponent(props.person_id ?? props.personId ?? props.id ?? props.personID);

const resolveOperaId = (props = {}) =>
  normalizeIdComponent(props.opera_id ?? props.operaId ?? props.id);

const resolveBookId = (props = {}) =>
  normalizeIdComponent(props.book_id ?? props.bookId ?? props.id);

const buildTypedId = (type, rawId, fallback) => {
  const normalizedType = String(type || '').trim().toLowerCase();
  if (!normalizedType) return '';
  const primary = normalizeIdComponent(rawId);
  if (primary) return `${normalizedType}:${primary}`;
  const fallbackValue = normalizeIdComponent(fallback);
  return fallbackValue ? `${normalizedType}:${fallbackValue.toLowerCase()}` : '';
};

// In-memory cache for pathfinding results
// Key format: `${fromNorm}|${toNorm}|${hops}` where names are lowercased & diacritics removed
const PATH_CACHE_TTL_MS = parseInt(process.env.PATH_CACHE_TTL_MS || '600000', 10); // 10 minutes
const PATH_CACHE_NEGATIVE_TTL_MS = parseInt(process.env.PATH_CACHE_NEGATIVE_TTL_MS || '120000', 10); // 2 minutes
const pathCache = new Map();
const normalizeNameForKey = (s) => (s || '')
  .toLowerCase()
  .replace(/ä/g, 'a')
  .replace(/ö/g, 'o')
  .replace(/ü/g, 'u')
  .replace(/ß/g, 'ss')
  .trim();
const getCachedPath = (from, to, hops) => {
  const key = `${normalizeNameForKey(from)}|${normalizeNameForKey(to)}|${hops}`;
  const now = Date.now();
  const entry = pathCache.get(key);
  if (!entry) return null;
  const ttl = entry.notFound ? PATH_CACHE_NEGATIVE_TTL_MS : PATH_CACHE_TTL_MS;
  if (now - entry.at > ttl) {
    pathCache.delete(key);
    return null;
  }
  return entry.data;
};
const setCachedPath = (from, to, hops, data, notFound = false) => {
  const key = `${normalizeNameForKey(from)}|${normalizeNameForKey(to)}|${hops}`;
  pathCache.set(key, { at: Date.now(), data, notFound });
};

// Lightweight caches for details endpoints (reduce repeated DB/graph lookups)
const makeCacheHelpers = () => {
  const get = (map, key, ttl) => {
    const entry = map.get(key);
    if (!entry) return null;
    if (Date.now() - entry.at > ttl) { map.delete(key); return null; }
    return entry.data;
  };
  const set = (map, key, value, max) => {
    map.set(key, { data: value, at: Date.now() });
    if (map.size > max) {
      const firstKey = map.keys().next().value;
      if (firstKey !== undefined) map.delete(firstKey);
    }
  };
  return { get, set };
};
const { get: cacheGet, set: cacheSet } = makeCacheHelpers();
const PERSON_CACHE = new Map(); // key: name|depth
const OPERA_CACHE = new Map(); // key: id|name
const BOOK_CACHE = new Map(); // key: title
const PERSON_TTL_MS = 5 * 60 * 1000;
const OPERA_TTL_MS = 5 * 60 * 1000;
const BOOK_TTL_MS = 5 * 60 * 1000;
const CACHE_MAX_ENTRIES = 200;

const normalizeRelationshipSourceValue = (value) => {
  if (value == null) return null;
  if (Array.isArray(value)) {
    for (const entry of value) {
      const normalized = normalizeRelationshipSourceValue(entry);
      if (normalized) return normalized;
    }
    return null;
  }
  if (typeof value === 'object') {
    if (typeof value.toString === 'function' && value.toString !== Object.prototype.toString) {
      const str = String(value).trim();
      return str || null;
    }
    try {
      const json = JSON.stringify(value);
      return json || null;
    } catch (_) {
      return null;
    }
  }
  const str = String(value).trim();
  return str || null;
};

const pickRelationshipSourceValue = (...values) => {
  for (const value of values) {
    const normalized = normalizeRelationshipSourceValue(value);
    if (normalized) return normalized;
  }
  return null;
};

// Periodic keep-alive ping to prevent Neo4j from pausing
let neo4jKeepAliveInterval = null;
const startNeo4jKeepAlive = () => {
  const intervalMs = parseInt(process.env.NEO4J_KEEPALIVE_MS || '43200000', 10); // default: 12 hours
  const ping = async () => {
    const session = driver.session();
    try {
      await session.run('RETURN 1 AS ok');
      console.log(`[Neo4j keepalive] OK @ ${new Date().toISOString()}`);
    } catch (err) {
      console.error('[Neo4j keepalive] Failed:', err?.message || err);
    } finally {
      try { await session.close(); } catch (_) {}
    }
  };
  // Initial ping on startup, then on interval
  ping();
  neo4jKeepAliveInterval = setInterval(ping, intervalMs);
};

// Middleware to verify JWT token
const authenticateToken = (req, res, next) => {
  console.log('authenticateToken called');
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) {
    console.error('No access token provided');
    return res.status(401).json({ error: 'Access token required' });
  }

  jwt.verify(token, JWT_SECRET, (err, payload) => {
    if (err) {
      console.error('JWT verification error:', err);
      return res.status(403).json({ error: 'Invalid or expired token' });
    }
    if (payload?.purpose === 'tos_accept') {
      return res.status(403).json({ error: 'Disclaimer acknowledgement pending', requiresTos: true });
    }
    if (payload?.tosVersion !== ACTIVE_TOS_VERSION) {
      return res.status(403).json({ error: 'Disclaimer acknowledgement required', requiresTos: true });
    }
    req.user = payload;
    next();
  });
};

// Health check endpoint
app.get('/ping', (req, res) => {
  res.json({ status: 'OK', timestamp: new Date().toISOString() });
});

// Authentication endpoints
app.post('/auth/login', async (req, res) => {
  await new Promise(resolve => authLimiter(req, res, resolve));
  const rawEmail = (req.body?.email || '').trim();
  const email = rawEmail.toLowerCase();
  const password = req.body?.password || '';

  try {
    if (!email || !password) {
      return res.status(400).json({ error: 'Email and password required' });
    }

    const pool = await getMysqlPool();
    const [rows] = await pool.query('SELECT id, password_hash, tos_version FROM users WHERE email = ?', [email]);
    if (rows.length === 0) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    const { password_hash: passwordHash, tos_version: tosVersion } = rows[0];
    const isValidPassword = await bcrypt.compare(password, passwordHash);
    if (!isValidPassword) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    if ((tosVersion || 0) < ACTIVE_TOS_VERSION) {
      const pendingToken = generatePendingTosToken(email);
      return res.status(403).json({
        error: 'Please review and accept the disclaimer to continue.',
        requiresTos: true,
        pendingToken,
        email
      });
    }

    const token = generateAuthToken(email);
    res.json({ token, email });
  } catch (error) {
    console.error('Login error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/session/token', async (req, res) => {
  if (!auth0Config) {
    return res.status(503).json({ error: 'Auth0 session bridge is not configured on this server.' });
  }
  try {
    if (!req.oidc || !req.oidc.isAuthenticated()) {
      return res.status(401).json({ error: 'Auth0 session not found. Please sign in again.' });
    }
    const email = (req.oidc.user?.email || '').trim().toLowerCase();
    if (!email) {
      return res.status(400).json({ error: 'Authenticated Auth0 profile is missing an email address.' });
    }

    const userRecord = await ensureUserRecordForEmail(email);
    if (!userRecord) {
      return res.status(500).json({ error: 'Unable to load linked user record.' });
    }

    // Determine ToS acceptance using configured strategy
    // 1) Prefer namespaced custom ID token claims (when set via an Auth0 Action)
    // 2) Fallback to app_metadata if present on the ID token (often not included by default)
    let claimTosVersion = Number(req.oidc.user?.[AUTH0_TOS_CLAIM] || 0);
    const claimTosAcceptedAt = req.oidc.user?.[AUTH0_TOS_ACCEPTED_AT_CLAIM] || null;
    let metadataTosVersion = Number((req.oidc.user?.app_metadata?.tos_version) || 0);
    let metadataTosAcceptedAt = (req.oidc.user?.app_metadata?.tos_accepted_at) || null;
    let effectiveTosVersion = Math.max(claimTosVersion, metadataTosVersion);
    let effectiveTosAcceptedAt = claimTosAcceptedAt || metadataTosAcceptedAt || null;
    // 3) If still unknown, consult Auth0 Management API for latest app_metadata (no Actions needed)
    if (effectiveTosVersion < ACTIVE_TOS_VERSION && AUTH0_MGMT_CLIENT_ID && AUTH0_MGMT_CLIENT_SECRET) {
      try {
        const u = await mgmtFindUserByEmail(email);
        const m = u && u.app_metadata ? u.app_metadata : {};
        const v = Number(m.tos_version || 0);
        if (Number.isFinite(v) && v > effectiveTosVersion) {
          metadataTosVersion = v;
          effectiveTosVersion = v;
          metadataTosAcceptedAt = m.tos_accepted_at || metadataTosAcceptedAt;
          effectiveTosAcceptedAt = claimTosAcceptedAt || metadataTosAcceptedAt || null;
        }
      } catch (e) {
        console.warn('[ToS] mgmt lookup failed:', e?.message || e);
      }
    }

    // Helper: promote Auth0 acceptance into MySQL if needed
    const promoteAuth0TosToMysqlIfNeeded = async () => {
      try {
        if (MYSQL_SYNC_TOS_TO_USERS && effectiveTosVersion >= ACTIVE_TOS_VERSION && (userRecord.tos_version || 0) < ACTIVE_TOS_VERSION) {
          const pool = await getMysqlPool();
          await pool.query(
            'UPDATE users SET tos_version = ?, tos_accepted_at = COALESCE(?, NOW()) WHERE email = ?',
            [ACTIVE_TOS_VERSION, effectiveTosAcceptedAt, email]
          );
        }
      } catch (e) {
        console.warn('[ToS] Failed to promote Auth0 ToS to MySQL:', e?.message || e);
      }
    };

    if (TOS_ENFORCEMENT_MODE === 'off') {
      await promoteAuth0TosToMysqlIfNeeded();
      // Ensure a public user id exists
      try { await getOrCreatePublicIdForEmail(email); } catch (_) {}
      const token = generateAuthToken(email);
      return res.json({ token, email });
    }

    if (TOS_ENFORCEMENT_MODE === 'auth0_only') {
      if (effectiveTosVersion >= ACTIVE_TOS_VERSION) {
        await promoteAuth0TosToMysqlIfNeeded();
        try { await getOrCreatePublicIdForEmail(email); } catch (_) {}
        const token = generateAuthToken(email);
        return res.json({ token, email });
      }
      const pendingToken = generatePendingTosToken(email);
      return res.status(403).json({
        error: 'Please review and accept the disclaimer to continue (Auth0).',
        requiresTos: true,
        email,
        pendingToken
      });
    }

    // legacy (default): enforce using MySQL, but honor Auth0 acceptance as authoritative
    if (effectiveTosVersion >= ACTIVE_TOS_VERSION) {
      await promoteAuth0TosToMysqlIfNeeded();
    }
    if ((userRecord.tos_version || 0) < ACTIVE_TOS_VERSION) {
      const pendingToken = generatePendingTosToken(email);
      return res.status(403).json({
        error: 'Please review and accept the disclaimer to continue.',
        requiresTos: true,
        pendingToken,
        email
      });
    }

    try { await getOrCreatePublicIdForEmail(email); } catch (_) {}
    const token = generateAuthToken(email);
    return res.json({ token, email });
  } catch (err) {
    console.error('[session/token] error:', err);
    return res.status(500).json({ error: 'Failed to synchronize Auth0 session' });
  }
});

app.post('/auth/accept-tos', async (req, res) => {
  await new Promise(resolve => authLimiter(req, res, resolve));
  try {
    if (!DB_WRITES_ENABLED || TOS_ENFORCEMENT_MODE === 'auth0_only') {
      // ToS managed by Auth0 or writes disabled; respond success without DB writes.
      // Derive email from provided email or from the pending acceptance token if available.
      let emailVal = (req.body?.email || '').trim().toLowerCase();
      if (!emailVal) {
        const raw = (req.body?.pendingToken || req.body?.token || '').trim();
        if (raw) {
          try {
            const payload = jwt.verify(raw, JWT_SECRET);
            if (payload && typeof payload.email === 'string') {
              emailVal = payload.email.trim().toLowerCase();
            }
          } catch (_) {
            // fall through with empty email
          }
        }
      }
      // Set a cookie to remember ToS acceptance on the backend domain (works across tunnels)
      // Best-effort: update Auth0 app_metadata so acceptance persists across devices
      if (emailVal && AUTH0_MGMT_CLIENT_ID && AUTH0_MGMT_CLIENT_SECRET) {
        try {
          const u = await mgmtFindUserByEmail(emailVal);
          if (u && u.user_id) {
            await mgmtUpdateUserAppMetadata(u.user_id, {
              tos_version: ACTIVE_TOS_VERSION,
              tos_accepted_at: new Date().toISOString()
            });
          }
        } catch (e) {
          console.warn('[ToS] mgmt update failed:', e?.message || e);
        }
      }
      const token = generateAuthToken(emailVal || '');
      return res.json({ success: true, token, email: emailVal || '' });
    }
    const token = (req.body?.pendingToken || req.body?.token || '').trim();
    if (!token) {
      return res.status(400).json({ error: 'Acceptance token required.' });
    }

    let payload;
    try {
      payload = jwt.verify(token, JWT_SECRET);
    } catch (err) {
      return res.status(401).json({ error: 'Invalid or expired acceptance token.' });
    }

    const email = (payload?.email || '').toLowerCase();
    if (!email || payload?.purpose !== 'tos_accept' || payload?.tosVersion !== ACTIVE_TOS_VERSION) {
      return res.status(400).json({ error: 'Malformed acceptance token.' });
    }

    const pool = await getMysqlPool();
    const [result] = await pool.query(
      'UPDATE users SET tos_version = ?, tos_accepted_at = NOW() WHERE email = ?',
      [ACTIVE_TOS_VERSION, email]
    );
    if (!result.affectedRows) {
      return res.status(404).json({ error: 'User account not found.' });
    }

    const authToken = generateAuthToken(email);
    res.json({ success: true, token: authToken, email });
  } catch (error) {
    console.error('Accept TOS error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/auth/forgot-password', async (req, res) => {
  if (!PASSWORD_RESET_ENABLED) {
    return res.status(503).json({ error: 'Password reset is managed by Auth0. Local reset endpoints are disabled.' });
  }
  if (!DB_WRITES_ENABLED) {
    return res.status(503).json({ error: 'Password reset is managed by Auth0. This server is in read-only mode.' });
  }
  await new Promise(resolve => authLimiter(req, res, resolve));
  try {
    const { email } = req.body || {};
    const normalizedEmail = (email || '').trim().toLowerCase();
    if (!normalizedEmail) {
      return res.status(400).json({ error: 'Email required' });
    }

    if (!isPasswordResetEmailConfigured()) {
      console.error('[Forgot Password] Email configuration incomplete.');
      return res.status(500).json({ error: 'Password reset is not configured.' });
    }

    const pool = await getMysqlPool();
    const [users] = await pool.query('SELECT id FROM users WHERE email = ?', [normalizedEmail]);

    if (users.length === 0) {
      // Respond with generic success to avoid account enumeration
      await new Promise((resolve) => setTimeout(resolve, 150));
      return res.json({ success: true });
    }

    await pool.query('DELETE FROM password_resets WHERE user_email = ? OR expires_at < NOW()', [normalizedEmail]);
    const { rawToken } = await createPasswordResetRecord(pool, normalizedEmail);
    try {
      await sendPasswordResetEmail(normalizedEmail, rawToken);
    } catch (err) {
      console.error('[Forgot Password] Failed to send email:', err?.response?.body || err);
      return res.status(500).json({ error: 'Failed to send reset email' });
    }

    res.json({ success: true });
  } catch (error) {
    console.error('Forgot password error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/auth/reset-password', async (req, res) => {
  if (!PASSWORD_RESET_ENABLED) {
    return res.status(503).json({ error: 'Password reset is managed by Auth0. Local reset endpoints are disabled.' });
  }
  if (!DB_WRITES_ENABLED) {
    return res.status(503).json({ error: 'Password reset is managed by Auth0. This server is in read-only mode.' });
  }
  await new Promise(resolve => authLimiter(req, res, resolve));
  try {
    const { token, password } = req.body || {};
    if (!token || typeof token !== 'string') {
      return res.status(400).json({ error: 'Reset token is required' });
    }
    if (!password || typeof password !== 'string' || password.length < 8) {
      return res.status(400).json({ error: 'Password must be at least 8 characters' });
    }

    const tokenHash = crypto.createHash('sha256').update(token).digest('hex');
    const pool = await getMysqlPool();
    const [records] = await pool.query(
      'SELECT id, user_email, expires_at, consumed_at FROM password_resets WHERE token_hash = ? LIMIT 1',
      [tokenHash]
    );

    if (records.length === 0) {
      return res.status(400).json({ error: 'Invalid or expired reset token' });
    }

    const record = records[0];
    if (record.consumed_at) {
      return res.status(400).json({ error: 'Reset token has already been used' });
    }
    const expiresAt = record.expires_at instanceof Date ? record.expires_at : new Date(record.expires_at);
    if (expiresAt < new Date()) {
      return res.status(400).json({ error: 'Reset token has expired' });
    }

    const hashedPassword = await bcrypt.hash(password, 10);
    const connection = await pool.getConnection();
    try {
      await connection.beginTransaction();
      const [updateResult] = await connection.query('UPDATE users SET password_hash = ? WHERE email = ?', [hashedPassword, record.user_email]);
      if (!updateResult.affectedRows) {
        throw new Error('Reset password attempted for missing user');
      }
      await connection.query('UPDATE password_resets SET consumed_at = NOW() WHERE id = ?', [record.id]);
      await connection.commit();
    } catch (err) {
      await connection.rollback();
      throw err;
    } finally {
      connection.release();
    }

    const normalizedEmail = (record.user_email || '').toLowerCase();
    const [userRows] = await pool.query('SELECT tos_version FROM users WHERE email = ? LIMIT 1', [normalizedEmail]);
    const tosVersion = userRows?.[0]?.tos_version || 0;
    if ((tosVersion || 0) < ACTIVE_TOS_VERSION) {
      const pendingToken = generatePendingTosToken(normalizedEmail);
      return res.json({ success: true, requiresTos: true, pendingToken, email: normalizedEmail });
    }

    const authToken = generateAuthToken(normalizedEmail);
    res.json({ success: true, token: authToken, email: normalizedEmail });
  } catch (error) {
    console.error('Reset password error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Add this debug endpoint before the existing endpoints
if (DEBUG_ENDPOINTS_ENABLED) app.post('/debug/corrupted-premieres', authenticateToken, async (req, res) => {
  const session = driver.session();
  try {
    // Find PREMIERED_ROLE_IN relationships pointing to Person nodes (should only point to Opera nodes)
    const result = await session.run(
      `MATCH (p1:Person)-[r:PREMIERED_ROLE_IN]->(p2:Person)
       RETURN p1.full_name as source_person, p1.person_id as source_id,
              p2.full_name as target_person, p2.person_id as target_id,
              r.role as role, r.source as source
       ORDER BY target_person`
    );

    const corruptedData = result.records.map(record => ({
      source_person: record.get('source_person'),
      source_id: record.get('source_id'),
      target_person: record.get('target_person'), 
      target_id: record.get('target_id'),
      role: record.get('role'),
      source: record.get('source')
    }));

    res.json({ corrupted_premieres: corruptedData });
  } catch (error) {
    console.error('Debug corrupted premieres error:', error);
    res.status(500).json({ error: 'Failed to fetch corrupted data' });
  } finally {
    await session.close();
  }
});

// Add another debug endpoint to find Eric Tappy in opera data
if (DEBUG_ENDPOINTS_ENABLED) app.post('/debug/eric-tappy-roles', authenticateToken, async (req, res) => {
  const session = driver.session();
  try {
    // Find all relationships involving Eric Tappy
    const result = await session.run(
      `MATCH (eric:Person {full_name: "Eric Tappy"})
       OPTIONAL MATCH (eric)-[r1:PREMIERED_ROLE_IN]->(o:Opera)
       OPTIONAL MATCH (someone)-[r2:PREMIERED_ROLE_IN]->(eric)
       OPTIONAL MATCH (eric)-[r3]-(connected)
       RETURN eric.person_id as eric_id,
              collect(DISTINCT {opera: o.opera_name, role: r1.role, source: r1.source}) as roles_in_operas,
              collect(DISTINCT {someone: someone.full_name, role: r2.role, source: r2.source}) as people_premiered_in_eric,
              collect(DISTINCT {type: type(r3), connected: connected.full_name || connected.opera_name || connected.title, connected_type: labels(connected)}) as all_connections`
    );

    const data = result.records[0];
    res.json({
      eric_id: data.get('eric_id'),
      roles_in_operas: data.get('roles_in_operas'),
      people_premiered_in_eric: data.get('people_premiered_in_eric'),
      all_connections: data.get('all_connections')
    });
  } catch (error) {
    console.error('Debug Eric Tappy error:', error);
    res.status(500).json({ error: 'Failed to fetch Eric Tappy data' });
  } finally {
    await session.close();
  }
});

// Add endpoint to find and fix the null-source premiere relationship
if (DEBUG_ENDPOINTS_ENABLED) app.post('/debug/fix-null-premiere', authenticateToken, async (req, res) => {
  const session = driver.session();
  try {
    const { action = 'find' } = req.body; // 'find' or 'delete'
    
    if (action === 'find') {
      // Find the problematic relationship
      const result = await session.run(
        `MATCH ()-[r:PREMIERED_ROLE_IN]->(eric:Person {full_name: "Eric Tappy"})
         RETURN startNode(r) as source_node, endNode(r) as target_node, 
                r.role as role, r.source as source, id(r) as relationship_id`
      );
      
      const problems = result.records.map(record => ({
        source_node: record.get('source_node'),
        target_node: record.get('target_node')?.properties,
        role: record.get('role'),
        source: record.get('source'),
        relationship_id: record.get('relationship_id')
      }));
      
      res.json({ problems, action: 'found' });
      
    } else if (action === 'delete') {
      // Delete the problematic relationship
      const result = await session.run(
        `MATCH ()-[r:PREMIERED_ROLE_IN]->(eric:Person {full_name: "Eric Tappy"})
         DELETE r
         RETURN count(r) as deleted_count`
      );
      
      const deletedCount = result.records[0].get('deleted_count');
      res.json({ deleted_count, action: 'deleted' });
    }
    
  } catch (error) {
    console.error('Debug fix null premiere error:', error);
    res.status(500).json({ error: 'Failed to fix null premiere' });
  } finally {
    await session.close();
  }
});

// Search endpoints
app.post('/search/singers', authenticateToken, async (req, res) => {
  console.log('Received /search/singers request:', req.body);
  const session = driver.session();
  try {
    const { query, limit } = req.body || {};
    const enforcedLimit = parseSearchLimit(limit);
    console.log('About to run Neo4j query');
    
    // Manual diacritical character replacement for German umlauts
    const cleanQuery = (query || '')
      .toLowerCase()
      .replace(/ä/g, 'a')
      .replace(/ö/g, 'o') 
      .replace(/ü/g, 'u')
      .replace(/ß/g, 'ss');
    
    const params = { query: query || '', cleanQuery };
    if (enforcedLimit) params.limit = neo4j.int(enforcedLimit);
    const cypherLimit = enforcedLimit ? 'LIMIT $limit' : '';
    const result = await session.run(
      `MATCH (s:Person) 
       WHERE apoc.text.clean(toLower(s.full_name)) CONTAINS apoc.text.clean(toLower($query))
          OR toLower(s.full_name) CONTAINS toLower($query)
          OR apoc.text.replace(apoc.text.replace(apoc.text.replace(apoc.text.replace(toLower(s.full_name), 'ä', 'a'), 'ö', 'o'), 'ü', 'u'), 'ß', 'ss') CONTAINS $cleanQuery
       RETURN s.full_name as name, s as properties
       ORDER BY s.full_name
       ${cypherLimit}`,
      params
    );
    console.log('Neo4j query complete');
    const singers = result.records.map(record => {
      const node = record.get('properties');
      const props = node?.properties || {};
      const personId = resolvePersonId(props);
      const fallbackId = props.full_name || props.name || record.get('name') || '';
      return {
        id: buildTypedId('person', personId, fallbackId),
        person_id: personId,
        name: record.get('name'),
        properties: props
      };
    });
    console.log('Sending singers response:', singers);
    res.json({ singers });
  } catch (error) {
    console.error('Singer search error:', error);
    res.status(500).json({ error: 'Failed to search singers' });
  } finally {
    try {
      await session.close();
    } catch (closeError) {
      console.error('Error closing Neo4j session:', closeError);
    }
  }
});

app.post('/search/operas', authenticateToken, async (req, res) => {
  const session = driver.session();
  try {
    const { query, limit } = req.body || {};
    const enforcedLimit = parseSearchLimit(limit);
    
    // Manual diacritical character replacement for German umlauts
    const cleanQuery = (query || '')
      .toLowerCase()
      .replace(/ä/g, 'a')
      .replace(/ö/g, 'o') 
      .replace(/ü/g, 'u')
      .replace(/ß/g, 'ss');
    
    const params = { query: query || '', cleanQuery };
    if (enforcedLimit) params.limit = neo4j.int(enforcedLimit);
    const cypherLimit = enforcedLimit ? 'LIMIT $limit' : '';
    const result = await session.run(
      `MATCH (o:Opera) 
       WHERE apoc.text.clean(toLower(o.opera_name)) CONTAINS apoc.text.clean(toLower($query))
          OR toLower(o.opera_name) CONTAINS toLower($query)
          OR apoc.text.replace(apoc.text.replace(apoc.text.replace(apoc.text.replace(toLower(o.opera_name), 'ä', 'a'), 'ö', 'o'), 'ü', 'u'), 'ß', 'ss') CONTAINS $cleanQuery
       RETURN o as properties
       ORDER BY o.opera_name
       ${cypherLimit}`,
      params
    );

    const operas = result.records.map(record => {
      const node = record.get('properties');
      const props = node?.properties || {};
      const operaId = resolveOperaId(props);
      const fallbackId = props.opera_name || props.title || props.name || '';
      return {
        id: buildTypedId('opera', operaId, fallbackId),
        opera_id: operaId,
        properties: props
      };
    });

    res.json({ operas });
  } catch (error) {
    console.error('Opera search error:', error);
    res.status(500).json({ error: 'Failed to search operas' });
  } finally {
    await session.close();
  }
});

app.post('/search/books', authenticateToken, async (req, res) => {
  const session = driver.session();
  try {
    const { query, limit } = req.body || {};
    const enforcedLimit = parseSearchLimit(limit);
    
    // Manual diacritical character replacement for German umlauts
    const cleanQuery = (query || '')
      .toLowerCase()
      .replace(/ä/g, 'a')
      .replace(/ö/g, 'o') 
      .replace(/ü/g, 'u')
      .replace(/ß/g, 'ss');
    
    const params = { query: query || '', cleanQuery };
    if (enforcedLimit) params.limit = neo4j.int(enforcedLimit);
    const cypherLimit = enforcedLimit ? 'LIMIT $limit' : '';
    const result = await session.run(
      `MATCH (b:Book) 
       WHERE apoc.text.clean(toLower(b.title)) CONTAINS apoc.text.clean(toLower($query))
          OR toLower(b.title) CONTAINS toLower($query)
          OR apoc.text.replace(apoc.text.replace(apoc.text.replace(apoc.text.replace(toLower(b.title), 'ä', 'a'), 'ö', 'o'), 'ü', 'u'), 'ß', 'ss') CONTAINS $cleanQuery
       RETURN b as properties
       ORDER BY b.title
       ${cypherLimit}`,
      params
    );

    const books = result.records.map(record => {
      const node = record.get('properties');
      const props = node?.properties || {};
      const bookId = resolveBookId(props);
      const fallbackId = props.title || props.name || '';
      return {
        id: buildTypedId('book', bookId, fallbackId),
        book_id: bookId,
        properties: props
      };
    });

    res.json({ books });
  } catch (error) {
    console.error('Book search error:', error);
    res.status(500).json({ error: 'Failed to search books' });
  } finally {
    await session.close();
  }
});

// Network endpoints
app.post('/singer/network', authenticateToken, async (req, res) => {
  await new Promise(resolve => networkLimiter(req, res, resolve));
  const session = driver.session();
  try {
    const { singerName, depth = 2 } = req.body;
    
    if (!singerName) {
      return res.status(400).json({ error: 'Singer name required' });
    }

    // Get singer details
    const singerResult = await session.run(
      'MATCH (s:Person {full_name: $name}) RETURN s',
      { name: singerName }
    );

    if (singerResult.records.length === 0) {
      return res.status(404).json({ error: 'Singer not found' });
    }

    const centerRaw = singerResult.records[0].get('s').properties;
    const centerPersonId = resolvePersonId(centerRaw);
    const center = {
      ...centerRaw,
      person_id: centerPersonId,
      id: buildTypedId('person', centerPersonId, centerRaw.full_name || centerRaw.name)
    };

    // Get teachers with relationship source
    const teachersResult = await session.run(
      `MATCH (s:Person {full_name: $name})<-[r:TAUGHT]-(t:Person)
       RETURN t AS teacher_node, r AS teacher_relationship`,
      { name: singerName }
    );
    const teachers = teachersResult.records.map(r => {
      const teacherNodeValue = r.get('teacher_node');
      const teacherRelValue = r.get('teacher_relationship');
      const teacherNodeRaw = teacherNodeValue ? teacherNodeValue.properties || {} : {};
      const teacherPersonId = resolvePersonId(teacherNodeRaw);
      const teacherNode = {
        ...teacherNodeRaw,
        person_id: teacherPersonId,
        id: buildTypedId('person', teacherPersonId, teacherNodeRaw.full_name || teacherNodeRaw.name)
      };
      const rawRelationshipProps = teacherRelValue ? teacherRelValue.properties || {} : {};
      const teacherRelSourceText = pickRelationshipSourceValue(
        rawRelationshipProps.teacher_rel_source_text,
        rawRelationshipProps.teacher_rel_source,
        rawRelationshipProps.relationship_source,
        rawRelationshipProps.source,
        rawRelationshipProps.relSource,
        rawRelationshipProps.label,
        rawRelationshipProps.text,
        rawRelationshipProps.notes,
        rawRelationshipProps.citation
      );
      const teacherRelSourceUrl = normalizeRelationshipSourceValue(rawRelationshipProps.teacher_rel_source_url);
      const teacherRelSource = teacherRelSourceText ||
        normalizeRelationshipSourceValue(rawRelationshipProps.teacher_rel_source) ||
        normalizeRelationshipSourceValue(rawRelationshipProps.relationship_source) ||
        normalizeRelationshipSourceValue(rawRelationshipProps.source) ||
        normalizeRelationshipSourceValue(rawRelationshipProps.relSource) ||
        normalizeRelationshipSourceValue(rawRelationshipProps.label) ||
        normalizeRelationshipSourceValue(rawRelationshipProps.text) ||
        normalizeRelationshipSourceValue(rawRelationshipProps.notes) ||
        normalizeRelationshipSourceValue(rawRelationshipProps.citation);
      console.log('[singer/network] mapped teacher', {
        name: teacherNode.full_name,
        derived_teacher_rel_source: teacherRelSource,
        raw_relationship: rawRelationshipProps
      });
      return {
        ...teacherNode,
        teacher_rel_source: teacherRelSource,
        teacher_rel_source_text: teacherRelSourceText || teacherRelSource || null,
        teacher_rel_source_url: teacherRelSourceUrl || null,
        relationship_properties: rawRelationshipProps,
        source:
          teacherNode.voice_type_source ||
          teacherNode.spelling_source ||
          teacherNode.dates_source ||
          teacherNode.birthplace_source ||
          teacherNode.image_source ||
          teacherNode.underrepresented_source ||
          'Unknown'
      };
    });

    // Get students with relationship source
    const studentsResult = await session.run(
      `MATCH (s:Person {full_name: $name})-[r:TAUGHT]->(st:Person)
       RETURN st AS student_node, r AS student_relationship`,
      { name: singerName }
    );
    const students = studentsResult.records.map(r => {
      const studentNodeValue = r.get('student_node');
      const studentRelValue = r.get('student_relationship');
      const studentNodeRaw = studentNodeValue ? studentNodeValue.properties || {} : {};
      const studentPersonId = resolvePersonId(studentNodeRaw);
      const studentNode = {
        ...studentNodeRaw,
        person_id: studentPersonId,
        id: buildTypedId('person', studentPersonId, studentNodeRaw.full_name || studentNodeRaw.name)
      };
      const rawRelationshipProps = studentRelValue ? studentRelValue.properties || {} : {};
      const teacherRelSourceText = pickRelationshipSourceValue(
        rawRelationshipProps.teacher_rel_source_text,
        rawRelationshipProps.teacher_rel_source,
        rawRelationshipProps.relationship_source,
        rawRelationshipProps.source,
        rawRelationshipProps.relSource,
        rawRelationshipProps.label,
        rawRelationshipProps.text,
        rawRelationshipProps.notes,
        rawRelationshipProps.citation
      );
      const teacherRelSourceUrl = normalizeRelationshipSourceValue(rawRelationshipProps.teacher_rel_source_url);
      const teacherRelSource = teacherRelSourceText ||
        normalizeRelationshipSourceValue(rawRelationshipProps.teacher_rel_source) ||
        normalizeRelationshipSourceValue(rawRelationshipProps.relationship_source) ||
        normalizeRelationshipSourceValue(rawRelationshipProps.source) ||
        normalizeRelationshipSourceValue(rawRelationshipProps.relSource) ||
        normalizeRelationshipSourceValue(rawRelationshipProps.label) ||
        normalizeRelationshipSourceValue(rawRelationshipProps.text) ||
        normalizeRelationshipSourceValue(rawRelationshipProps.notes) ||
        normalizeRelationshipSourceValue(rawRelationshipProps.citation);
      console.log('[singer/network] mapped student', {
        name: studentNode.full_name,
        derived_teacher_rel_source: teacherRelSource,
        raw_relationship: rawRelationshipProps
      });
      return {
        ...studentNode,
        teacher_rel_source: teacherRelSource,
        teacher_rel_source_text: teacherRelSourceText || teacherRelSource || null,
        teacher_rel_source_url: teacherRelSourceUrl || null,
        relationship_properties: rawRelationshipProps,
        source:
          studentNode.voice_type_source ||
          studentNode.spelling_source ||
          studentNode.dates_source ||
          studentNode.birthplace_source ||
          studentNode.image_source ||
          studentNode.underrepresented_source ||
          'Unknown'
      };
    });

    // Get family with relationship source across explicit family relationship types
    const familyQuery = `
      MATCH (s:Person {full_name: $name})
      WITH s
      MATCH (p:Person)-[r1:PARENT]->(s)
      RETURN p AS f, 'parent' AS relationship, coalesce(
        r1.teacher_rel_source,
        r1.relationship_source,
        r1.relationshipSource,
        r1.relSource,
        r1.source,
        r1.notes,
        r1.text,
        r1.label,
        r1.citation
      ) AS teacher_rel_source,
      r1.teacher_rel_source_text AS teacher_rel_source_text,
      r1.teacher_rel_source_url AS teacher_rel_source_url
      UNION
      MATCH (s:Person {full_name: $name})-[r2:PARENT]->(c:Person)
      RETURN c AS f, 'parentOf' AS relationship, coalesce(
        r2.teacher_rel_source,
        r2.relationship_source,
        r2.relationshipSource,
        r2.relSource,
        r2.source,
        r2.notes,
        r2.text,
        r2.label,
        r2.citation
      ) AS teacher_rel_source,
      r2.teacher_rel_source_text AS teacher_rel_source_text,
      r2.teacher_rel_source_url AS teacher_rel_source_url
      UNION
      MATCH (s:Person {full_name: $name})-[r3:SIBLING]-(sib:Person)
      RETURN sib AS f, 'sibling' AS relationship, coalesce(
        r3.teacher_rel_source,
        r3.relationship_source,
        r3.relationshipSource,
        r3.relSource,
        r3.source,
        r3.notes,
        r3.text,
        r3.label,
        r3.citation
      ) AS teacher_rel_source,
      r3.teacher_rel_source_text AS teacher_rel_source_text,
      r3.teacher_rel_source_url AS teacher_rel_source_url
      UNION
      MATCH (s:Person {full_name: $name})-[r4:SPOUSE]-(sp:Person)
      RETURN sp AS f, 'spouse' AS relationship, coalesce(
        r4.teacher_rel_source,
        r4.relationship_source,
        r4.relationshipSource,
        r4.relSource,
        r4.source,
        r4.notes,
        r4.text,
        r4.label,
        r4.citation
      ) AS teacher_rel_source,
      r4.teacher_rel_source_text AS teacher_rel_source_text,
      r4.teacher_rel_source_url AS teacher_rel_source_url
      UNION
      MATCH (gp:Person)-[r5:GRANDPARENT]->(s:Person {full_name: $name})
      RETURN gp AS f, 'grandparent' AS relationship, coalesce(
        r5.teacher_rel_source,
        r5.relationship_source,
        r5.relationshipSource,
        r5.relSource,
        r5.source,
        r5.notes,
        r5.text,
        r5.label,
        r5.citation
      ) AS teacher_rel_source,
      r5.teacher_rel_source_text AS teacher_rel_source_text,
      r5.teacher_rel_source_url AS teacher_rel_source_url
      UNION
      MATCH (s:Person {full_name: $name})-[r6:GRANDPARENT]->(gc:Person)
      RETURN gc AS f, 'grandparentOf' AS relationship, coalesce(
        r6.teacher_rel_source,
        r6.relationship_source,
        r6.relationshipSource,
        r6.relSource,
        r6.source,
        r6.notes,
        r6.text,
        r6.label,
        r6.citation
      ) AS teacher_rel_source,
      r6.teacher_rel_source_text AS teacher_rel_source_text,
      r6.teacher_rel_source_url AS teacher_rel_source_url
    `;
    const familyResult = await session.run(familyQuery, { name: singerName });
    const family = familyResult.records.map(r => {
      const familyRaw = r.get('f').properties;
      const familyPersonId = resolvePersonId(familyRaw);
      const f = {
        ...familyRaw,
        person_id: familyPersonId,
        id: buildTypedId('person', familyPersonId, familyRaw.full_name || familyRaw.name)
      };
      const rel = r.get('relationship');
      const relSrcRaw = r.get('teacher_rel_source');
      const relSrc = normalizeRelationshipSourceValue(relSrcRaw);
      const relSrcTextRaw = r.get('teacher_rel_source_text');
      const relSrcUrlRaw = r.get('teacher_rel_source_url');
      const relSrcText = normalizeRelationshipSourceValue(relSrcTextRaw) || relSrc;
      const relSrcUrl = normalizeRelationshipSourceValue(relSrcUrlRaw);
      return {
        ...f,
        relationship_type: rel,
        teacher_rel_source: relSrcText || null,
        teacher_rel_source_text: relSrcText || null,
        teacher_rel_source_url: relSrcUrl || null,
        source: f.voice_type_source || f.spelling_source || f.dates_source || f.birthplace_source || f.image_source || f.underrepresented_source || 'Unknown'
      };
    });

    // Get works (operas and books)
    const operasResult = await session.run(
      'MATCH (s:Person {full_name: $name})-[r:PREMIERED_ROLE_IN]->(o:Opera) RETURN o.opera_name as opera_name, o.opera_id as opera_id, r.role as role, r.opera_source_text as opera_source_text, r.opera_source_url as opera_source_url, r.source as legacy_source',
      { name: singerName }
    );
    const operas = operasResult.records.map(r => {
      const operaId = normalizeIdComponent(r.get('opera_id'));
      const sourceTextRaw = r.get('opera_source_text');
      const sourceUrlRaw = r.get('opera_source_url');
      const legacySourceRaw = r.get('legacy_source');
      const resolvedSourceText = pickRelationshipSourceValue(sourceTextRaw, legacySourceRaw);
      const resolvedSourceUrl = normalizeRelationshipSourceValue(sourceUrlRaw);
      return {
        id: buildTypedId('opera', operaId, r.get('opera_name')),
        opera_id: operaId || null,
        opera_name: r.get('opera_name'),
        role: r.get('role'),
        source: resolvedSourceText || null,
        opera_source_text: resolvedSourceText || null,
        opera_source_url: resolvedSourceUrl || null
      };
    });

    // Get specific roles premiered (for the new Roles premiered card)
    const premieredRolesResult = await session.run(
      'MATCH (s:Person {full_name: $name})-[r:PREMIERED_ROLE_IN]->(o:Opera) RETURN o.opera_name as opera_name, o.opera_id as opera_id, r.role as role, r.opera_source_text as opera_source_text, r.opera_source_url as opera_source_url, r.source as legacy_source',
      { name: singerName }
    );
    const premieredRoles = premieredRolesResult.records.map(r => {
      const operaId = normalizeIdComponent(r.get('opera_id'));
      const sourceTextRaw = r.get('opera_source_text');
      const sourceUrlRaw = r.get('opera_source_url');
      const legacySourceRaw = r.get('legacy_source');
      const resolvedSourceText = pickRelationshipSourceValue(sourceTextRaw, legacySourceRaw);
      const resolvedSourceUrl = normalizeRelationshipSourceValue(sourceUrlRaw);
      return {
        id: buildTypedId('opera', operaId, r.get('opera_name')),
        opera_id: operaId || null,
        opera_name: r.get('opera_name'),
        role: r.get('role'),
        source: resolvedSourceText || null,
        opera_source_text: resolvedSourceText || null,
        opera_source_url: resolvedSourceUrl || null
      };
    });

    const booksResult = await session.run(
      `MATCH (s:Person {full_name: $name})-[r:AUTHORED]->(b:Book)
       RETURN b.title as title, b.book_id AS book_id, r AS relationship`,
      { name: singerName }
    );
    const books = booksResult.records.map(r => {
      const bookId = normalizeIdComponent(r.get('book_id'));
      return {
        id: buildTypedId('book', bookId, r.get('title')),
        book_id: bookId || null,
        title: r.get('title')
      };
    });

    // Get books the singer edited
    const editedBooksResult = await session.run(
      `MATCH (s:Person {full_name: $name})-[r:EDITED]->(b:Book)
       RETURN b.title as title, b.book_id AS book_id, r AS relationship`,
      { name: singerName }
    );
    const editedBooks = editedBooksResult.records.map(r => {
      const bookId = normalizeIdComponent(r.get('book_id'));
      return {
        id: buildTypedId('book', bookId, r.get('title')),
        book_id: bookId || null,
        title: r.get('title')
      };
    });

    const composedOperasResult = await session.run(
      `MATCH (s:Person {full_name: $name})-[r:COMPOSED|WROTE]->(o:Opera)
       RETURN o AS opera_node,
              o.opera_name AS opera_name,
              o.title AS title,
              o.name AS name,
              o.operaTitle AS operaTitle,
              o.label AS label,
              o.version AS version,
              o.opera_id AS opera_id,
              r AS relationship`,
      { name: singerName }
    );
    console.log('[debug composedOperas raw] count', composedOperasResult.records.length);
    const composedOperasRaw = composedOperasResult.records.map((r, index) => {
      const operaNodeValue = r.get('opera_node');
      const operaProps = operaNodeValue ? operaNodeValue.properties || {} : {};
      const relationshipValue = r.get('relationship');
      const relationshipProps = relationshipValue ? relationshipValue.properties || {} : {};
      console.log('[debug composedOperas entry]', index, {
        operaProps,
        relationshipProps
      });

      const directOperaValues = {
        opera_name: toPlainString(r.get('opera_name')),
        title: toPlainString(r.get('title')),
        name: toPlainString(r.get('name')),
        operaTitle: toPlainString(r.get('operaTitle')),
        label: toPlainString(r.get('label')),
        version: toPlainString(r.get('version')),
        opera_id: r.get('opera_id')
      };

      const sourceText = pickRelationshipSourceValue(
        relationshipProps.opera_source_text,
        relationshipProps.opera_source,
        relationshipProps.source,
        relationshipProps.relationship_source,
        relationshipProps.relSource,
        relationshipProps.label,
        relationshipProps.text,
        relationshipProps.notes,
        relationshipProps.citation
      );
      const sourceUrl = normalizeRelationshipSourceValue(relationshipProps.opera_source_url);

      const nameCandidates = [
        directOperaValues.title,
        directOperaValues.opera_name,
        directOperaValues.name,
        directOperaValues.operaTitle,
        directOperaValues.label,
        toPlainString(relationshipProps.opera_title),
        toPlainString(relationshipProps.opera_name),
        toPlainString(relationshipProps.title),
        toPlainString(relationshipProps.name),
        toPlainString(relationshipProps.target),
        toPlainString(operaProps.opera_name),
        toPlainString(operaProps.title),
        toPlainString(operaProps.name),
        toPlainString(operaProps.operaTitle),
        toPlainString(operaProps.label)
      ].filter(Boolean);

      const resolvedTitle = nameCandidates.length > 0 ? nameCandidates[0] : null;
      const resolvedOperaName = directOperaValues.opera_name || toPlainString(operaProps.opera_name);
      const resolvedVersion = directOperaValues.version || toPlainString(operaProps.version);
      const resolvedNameCandidate =
        directOperaValues.name ||
        resolvedTitle ||
        directOperaValues.operaTitle ||
        directOperaValues.label ||
        toPlainString(operaProps.name) ||
        toPlainString(operaProps.operaTitle) ||
        toPlainString(operaProps.label);

      const resolvedIdCandidate =
        directOperaValues.opera_id ??
        operaProps.opera_id ??
        operaProps.id ??
        null;

      const stringOperaId = resolvedIdCandidate !== null && resolvedIdCandidate !== undefined
        ? normalizeIdComponent(resolvedIdCandidate)
        : '';
      const resolvedDisplayName =
        resolvedNameCandidate ||
        (stringOperaId
          ? `Opera ${stringOperaId}`
          : null);

      return {
        id: buildTypedId('opera', stringOperaId, resolvedNameCandidate || resolvedTitle || resolvedDisplayName),
        title: resolvedTitle,
        opera_name: resolvedOperaName,
        name: resolvedNameCandidate || resolvedDisplayName,
        display_name: resolvedDisplayName,
        operaTitle: directOperaValues.operaTitle || toPlainString(operaProps.operaTitle),
        label: directOperaValues.label || toPlainString(operaProps.label),
        version: resolvedVersion,
        opera_id: stringOperaId || null,
        source: sourceText || null,
        opera_source_text: sourceText || null,
        opera_source_url: sourceUrl || null
      };
    });
    const composedOperas = [];
    const seenComposedTitles = new Set();
    for (const opera of composedOperasRaw) {
      const key =
        (opera.opera_id ? `id:${opera.opera_id}` : '') ||
        toPlainString(opera.title) ||
        toPlainString(opera.opera_name) ||
        toPlainString(opera.display_name);
      if (key && seenComposedTitles.has(key)) continue;
      if (key) {
        seenComposedTitles.add(key);
      }
      composedOperas.push(opera);
    }

    const payload = {
      center,
      teachers,
      students,
      family,
      premieredRoles,
      works: {
        operas,
        books,
        editedBooks,
        composedOperas
      }
    };
    try { cacheSet(PERSON_CACHE, `${(singerName || '').toLowerCase()}|${Number(depth) || 0}`, payload, CACHE_MAX_ENTRIES); } catch (_) {}
    res.json(payload);
  } catch (error) {
    console.error('Singer network error:', error);
    res.status(500).json({ error: 'Failed to fetch singer network' });
  } finally {
    await session.close();
  }
});

app.post('/node/relationship-counts', authenticateToken, async (req, res) => {
  const session = driver.session();
  try {
    const { nodeType, nodeName } = req.body || {};
    const trimmedType = typeof nodeType === 'string' ? nodeType.trim().toLowerCase() : '';
    const trimmedName = typeof nodeName === 'string' ? nodeName.trim() : '';

    if (!trimmedType || !trimmedName) {
      return res.status(400).json({ error: 'nodeType and nodeName are required' });
    }

    if (trimmedType === 'person') {
      const result = await session.run(
        `
        MATCH (s:Person {full_name: $name})
        WITH s
        OPTIONAL MATCH (s)<-[:TAUGHT]-(teacher:Person)
        WITH s, count(DISTINCT teacher) AS taughtBy
        OPTIONAL MATCH (s)-[:TAUGHT]->(student:Person)
        WITH s, taughtBy, count(DISTINCT student) AS taught
        OPTIONAL MATCH (s)-[:AUTHORED]->(book:Book)
        WITH s, taughtBy, taught, count(DISTINCT book) AS authored
        OPTIONAL MATCH (s)-[:EDITED]->(editedBook:Book)
        WITH s, taughtBy, taught, authored, count(DISTINCT editedBook) AS edited
        OPTIONAL MATCH (s)-[:PREMIERED_ROLE_IN]->(premOpera:Opera)
        WITH s, taughtBy, taught, authored, edited, count(DISTINCT premOpera) AS premieredRoleIn
        OPTIONAL MATCH (s)-[:COMPOSED|WROTE]->(wroteOpera:Opera)
        WITH s, taughtBy, taught, authored, edited, premieredRoleIn, count(DISTINCT wroteOpera) AS wrote
        OPTIONAL MATCH (parent:Person)-[:PARENT]->(s)
        WITH s, taughtBy, taught, authored, edited, premieredRoleIn, wrote, count(DISTINCT parent) AS parent
        OPTIONAL MATCH (s)-[:PARENT]->(child:Person)
        WITH s, taughtBy, taught, authored, edited, premieredRoleIn, wrote, parent, count(DISTINCT child) AS parentOf
        OPTIONAL MATCH (grandparent:Person)-[:GRANDPARENT]->(s)
        WITH s, taughtBy, taught, authored, edited, premieredRoleIn, wrote, parent, parentOf, count(DISTINCT grandparent) AS grandparent
        OPTIONAL MATCH (s)-[:GRANDPARENT]->(grandchild:Person)
        WITH s, taughtBy, taught, authored, edited, premieredRoleIn, wrote, parent, parentOf, grandparent, count(DISTINCT grandchild) AS grandparentOf
        OPTIONAL MATCH (s)-[:SPOUSE]-(spouseNode:Person)
        WITH s, taughtBy, taught, authored, edited, premieredRoleIn, wrote, parent, parentOf, grandparent, grandparentOf, count(DISTINCT spouseNode) AS spouse
        OPTIONAL MATCH (s)-[:SIBLING]-(siblingNode:Person)
        RETURN
          taughtBy,
          taught,
          authored,
          edited,
          premieredRoleIn,
          wrote,
          parent,
          parentOf,
          grandparent,
          grandparentOf,
          spouse,
          count(DISTINCT siblingNode) AS sibling
        `,
        { name: trimmedName }
      );

      if (result.records.length === 0) {
        return res.status(404).json({ error: 'Person not found' });
      }

      const record = result.records[0];
      return res.json({
        nodeType: 'person',
        nodeName: trimmedName,
        counts: {
          taughtBy: toPlainNumber(record.get('taughtBy')),
          taught: toPlainNumber(record.get('taught')),
          authored: toPlainNumber(record.get('authored')),
          edited: toPlainNumber(record.get('edited')),
          premieredRoleIn: toPlainNumber(record.get('premieredRoleIn')),
          wrote: toPlainNumber(record.get('wrote')),
          parent: toPlainNumber(record.get('parent')),
          parentOf: toPlainNumber(record.get('parentOf')),
          grandparent: toPlainNumber(record.get('grandparent')),
          grandparentOf: toPlainNumber(record.get('grandparentOf')),
          spouse: toPlainNumber(record.get('spouse')),
          sibling: toPlainNumber(record.get('sibling')),
          editedBy: 0
        }
      });
    }

    if (trimmedType === 'opera') {
      const result = await session.run(
        `
        MATCH (o:Opera {opera_name: $name})
        WITH o
        OPTIONAL MATCH (person:Person)-[:PREMIERED_ROLE_IN]->(o)
        WITH o, count(DISTINCT person) AS premieredRoleIn
        OPTIONAL MATCH (composer:Person)-[:COMPOSED|WROTE]->(o)
        RETURN premieredRoleIn, count(DISTINCT composer) AS wrote
        `,
        { name: trimmedName }
      );

      if (result.records.length === 0) {
        return res.status(404).json({ error: 'Opera not found' });
      }

      const record = result.records[0];
      return res.json({
        nodeType: 'opera',
        nodeName: trimmedName,
        counts: {
          taughtBy: 0,
          taught: 0,
          authored: 0,
          edited: 0,
          premieredRoleIn: toPlainNumber(record.get('premieredRoleIn')),
          wrote: toPlainNumber(record.get('wrote')),
          parent: 0,
          parentOf: 0,
          grandparent: 0,
          grandparentOf: 0,
          spouse: 0,
          sibling: 0,
          editedBy: 0
        }
      });
    }

    if (trimmedType === 'book') {
      const result = await session.run(
        `
        MATCH (b:Book {title: $name})
        WITH b
        OPTIONAL MATCH (author:Person)-[:AUTHORED]->(b)
        WITH b, count(DISTINCT author) AS authored
        OPTIONAL MATCH (editor:Person)-[:EDITED]->(b)
        RETURN authored, count(DISTINCT editor) AS edited
        `,
        { name: trimmedName }
      );

      if (result.records.length === 0) {
        return res.status(404).json({ error: 'Book not found' });
      }

      const record = result.records[0];
      return res.json({
        nodeType: 'book',
        nodeName: trimmedName,
        counts: {
          taughtBy: 0,
          taught: 0,
          authored: toPlainNumber(record.get('authored')),
          edited: toPlainNumber(record.get('edited')),
          premieredRoleIn: 0,
          wrote: 0,
          parent: 0,
          parentOf: 0,
          grandparent: 0,
          grandparentOf: 0,
          spouse: 0,
          sibling: 0,
          editedBy: 0
        }
      });
    }

    return res.status(400).json({ error: `Unsupported nodeType: ${nodeType}` });
  } catch (error) {
    console.error('Relationship count error:', error);
    res.status(500).json({ error: 'Failed to fetch relationship counts' });
  } finally {
    await session.close();
  }
});

app.post('/opera/details', authenticateToken, async (req, res) => {
  await new Promise(resolve => detailsLimiter(req, res, resolve));
  const session = driver.session();
  try {
    const { operaName, operaId: operaIdInput, opera_id: operaIdAlt } = req.body || {};
    const trimmedName = toPlainString(operaName);
    const operaIdRaw = normalizeIdComponent(operaIdInput ?? operaIdAlt);
    const hasName = Boolean(trimmedName);
    const hasId = Boolean(operaIdRaw);
    const numericId = hasId ? Number(operaIdRaw) : NaN;
    const hasNumericId = hasId && Number.isFinite(numericId);

    if (!hasName && !hasId) {
      return res.status(400).json({ error: 'Opera identifier (ID or name) required' });
    }

    // Get opera details using ID when available, name as a fallback
    const cacheKey = `opera:${(operaIdRaw || '').toLowerCase()}|${(trimmedName || '').toLowerCase()}`;
    const cached = cacheGet(OPERA_CACHE, cacheKey, OPERA_TTL_MS);
    if (cached) {
      return res.json(cached);
    }
    const operaResult = await session.run(
      `
      MATCH (o:Opera)
      WHERE
        ($hasId AND (
          (o.opera_id IS NOT NULL AND (
            ($hasNumericId AND o.opera_id = $operaIdNumeric) OR
            toString(o.opera_id) = $operaIdString
          ))
        ))
        OR
        ($hasName AND toLower(o.opera_name) = $lowerOperaName)
      RETURN o
      ORDER BY
        CASE
          WHEN $hasId AND (
            ($hasNumericId AND o.opera_id = $operaIdNumeric) OR
            toString(o.opera_id) = $operaIdString
          ) THEN 0
          ELSE 1
        END,
        o.opera_name
      LIMIT 1
      `,
      {
        hasId,
        hasNumericId,
        hasName,
        operaIdNumeric: hasNumericId ? neo4j.int(numericId) : null,
        operaIdString: operaIdRaw || null,
        lowerOperaName: trimmedName ? trimmedName.toLowerCase() : null
      }
    );

    if (operaResult.records.length === 0) {
      return res.status(404).json({ error: 'Opera not found' });
    }

    const operaNode = operaResult.records[0].get('o');
    const opera = operaNode.properties || {};
    const operaInternalId = operaNode.identity;
    const operaNodeIdParam = operaInternalId;

    // Try to resolve composer from relationships if not present as a property
    let composerName = opera.composer || null;
    try {
      const composerResult = await session.run(
        `
        MATCH (c:Person)-[r]->(o:Opera)
        WHERE id(o) = $operaNodeId
          AND type(r) IN ['COMPOSED','WROTE']
        RETURN c.full_name AS composer
        LIMIT 1
        `,
        { operaNodeId: operaNodeIdParam }
      );
      if (composerResult.records.length > 0) {
        composerName = composerResult.records[0].get('composer') || composerName;
      }
    } catch (_) {}

    // Get premiered roles
    const rolesResult = await session.run(
      `
      MATCH (s:Person)-[r:PREMIERED_ROLE_IN]->(o:Opera)
      WHERE id(o) = $operaNodeId
      RETURN s.full_name as singer, r.role as role, s.voice_type as voice_type,
        s.voice_type_source as voice_type_source, s.spelling_source as spelling_source, s.dates_source as dates_source, s.birthplace_source as birthplace_source, s.image_source as image_source, s.underrepresented_source as underrepresented_source,
        r.opera_source_text as opera_source_text, r.opera_source_url as opera_source_url, r.source as relationship_source
      `,
      { operaNodeId: operaNodeIdParam }
    );
    const premieredRoles = rolesResult.records.map(r => {
      const sourceText = pickRelationshipSourceValue(r.get('opera_source_text'), r.get('relationship_source'));
      const sourceUrl = normalizeRelationshipSourceValue(r.get('opera_source_url'));
      return {
        singer: r.get('singer'),
        role: r.get('role'),
        voice_type: r.get('voice_type'),
        source: sourceText || null,
        opera_source_text: sourceText || null,
        opera_source_url: sourceUrl || null
      };
    });

    // Get composers via WROTE relationship (source stored on relationship)
    const wroteResult = await session.run(
      `
      MATCH (c:Person)-[w:WROTE]->(o:Opera)
      WHERE id(o) = $operaNodeId
      RETURN c.full_name as composer, w.opera_source_text as opera_source_text, w.opera_source_url as opera_source_url, w.source as relationship_source
      ORDER BY composer
      `,
      { operaNodeId: operaNodeIdParam }
    );
    const wrote = wroteResult.records.map(r => {
      const sourceText = pickRelationshipSourceValue(r.get('opera_source_text'), r.get('relationship_source'));
      const sourceUrl = normalizeRelationshipSourceValue(r.get('opera_source_url'));
      return {
        composer: r.get('composer'),
        source: sourceText || null,
        opera_source_text: sourceText || null,
        opera_source_url: sourceUrl || null
      };
    });

    const payload = {
      opera: {
        id: buildTypedId('opera', resolveOperaId(opera), opera.opera_name || trimmedName || operaIdRaw),
        opera_id: resolveOperaId(opera) || null,
        opera_name: opera.opera_name,
        composer: composerName,
        premiere_year: opera.premiere_year
      },
      premieredRoles,
      wrote
    };
    try { cacheSet(OPERA_CACHE, cacheKey, payload, CACHE_MAX_ENTRIES); } catch (_) {}
    res.json(payload);
  } catch (error) {
    console.error('Opera details error:', error);
    res.status(500).json({ error: 'Failed to fetch opera details' });
  } finally {
    await session.close();
  }
});

app.post('/book/details', authenticateToken, async (req, res) => {
  await new Promise(resolve => detailsLimiter(req, res, resolve));
  const session = driver.session();
  try {
    const { bookTitle } = req.body;
    
    if (!bookTitle) {
      return res.status(400).json({ error: 'Book title required' });
    }
    const cacheKey = `book:${(bookTitle || '').toLowerCase()}`;
    const cached = cacheGet(BOOK_CACHE, cacheKey, BOOK_TTL_MS);
    if (cached) {
      return res.json(cached);
    }
    // Get book details
    const bookResult = await session.run(
      'MATCH (b:Book {title: $title}) RETURN b',
      { title: bookTitle }
    );

    if (bookResult.records.length === 0) {
      return res.status(404).json({ error: 'Book not found' });
    }

    const book = bookResult.records[0].get('b').properties;

    // Get authors
    const authorsResult = await session.run(
      `MATCH (s:Person)-[r:AUTHORED]->(b:Book {title: $title})
       RETURN s.full_name as author, s.voice_type as voice_type,
         s.voice_type_source as voice_type_source, s.spelling_source as spelling_source, s.dates_source as dates_source, s.birthplace_source as birthplace_source, s.image_source as image_source, s.underrepresented_source as underrepresented_source, r.source as relationship_source`,
      { title: bookTitle }
    );
    const authors = authorsResult.records.map(r => ({
      author: r.get('author'),
      voice_type: r.get('voice_type')
    }));

    // Get editors
    const editorsResult = await session.run(
      `MATCH (s:Person)-[r:EDITED]->(b:Book {title: $title})
       RETURN s.full_name as editor, s.voice_type as voice_type,
         s.voice_type_source as voice_type_source, s.spelling_source as spelling_source, s.dates_source as dates_source, s.birthplace_source as birthplace_source, s.image_source as image_source, s.underrepresented_source as underrepresented_source, r.source as relationship_source`,
      { title: bookTitle }
    );
    const editors = editorsResult.records.map(r => ({
      editor: r.get('editor'),
      voice_type: r.get('voice_type')
    }));

    const payload = {
      book: {
        title: book.title,
        normalized_title: book.normalized_title,
        type: book.type,
        link: book.link,
        book_id: book.book_id
      },
      authors,
      editors
    };
    try { cacheSet(BOOK_CACHE, cacheKey, payload, CACHE_MAX_ENTRIES); } catch (_) {}
    res.json(payload);
  } catch (error) {
    console.error('Book details error:', error);
    res.status(500).json({ error: 'Failed to fetch book details' });
  } finally {
    await session.close();
  }
});

// Path finding between two people (demo)
app.post('/path/find', authenticateToken, async (req, res) => {
  await new Promise(resolve => networkLimiter(req, res, resolve));
  const session = driver.session();
  const startedAt = Date.now();
  try {
    const { from, to, maxHops } = req.body;
    if (!from || !to) {
      return res.status(400).json({ error: 'from and to (full_name) are required' });
    }
    const hops = Math.min(Math.max(parseInt(maxHops || 25, 10), 1), 30);
    const cached = getCachedPath(from, to, hops);
    if (cached) {
      console.log(`[path/find] cache HIT in ${Date.now() - startedAt}ms`, { from, to, hops });
      return res.json(cached);
    }
    console.log('[path/find] cache MISS, querying…', { from, to, hops });

    // Single-query resolution + shortest path to minimize round-trips
    const optimizedQuery = `
      WITH toLower($from) AS qFrom, toLower($to) AS qTo,
           apoc.text.clean(toLower($from)) AS cFrom,
           apoc.text.clean(toLower($to)) AS cTo
      CALL {
        WITH qFrom, cFrom
        MATCH (p:Person)
        WHERE toLower(p.full_name) = qFrom
           OR apoc.text.clean(toLower(p.full_name)) = cFrom
           OR apoc.text.clean(toLower(p.full_name)) CONTAINS cFrom
           OR cFrom CONTAINS apoc.text.clean(toLower(p.full_name))
        RETURN p AS fromNode
        ORDER BY CASE WHEN toLower(fromNode.full_name) = qFrom THEN 0 ELSE 1 END, size(fromNode.full_name)
        LIMIT 1
      }
      CALL {
        WITH qTo, cTo
        MATCH (p:Person)
        WHERE toLower(p.full_name) = qTo
           OR apoc.text.clean(toLower(p.full_name)) = cTo
           OR apoc.text.clean(toLower(p.full_name)) CONTAINS cTo
           OR cTo CONTAINS apoc.text.clean(toLower(p.full_name))
        RETURN p AS toNode
        ORDER BY CASE WHEN toLower(toNode.full_name) = qTo THEN 0 ELSE 1 END, size(toNode.full_name)
        LIMIT 1
      }
      WITH fromNode, toNode
      OPTIONAL MATCH p = shortestPath((fromNode)-[:TAUGHT|PARENT|SPOUSE|SIBLING|GRANDPARENT|PREMIERED_ROLE_IN|AUTHORED|WROTE|EDITED*..${hops}]-(toNode))
      WITH p
      WHERE p IS NOT NULL
      WITH nodes(p) AS ns, relationships(p) AS rs
      WITH ns, rs, [i IN range(0, size(rs)-1) | { sNode: ns[i], tNode: ns[i+1], r: rs[i] }] AS segs
      UNWIND segs AS seg
      WITH ns, seg.sNode AS sNode, seg.tNode AS tNode, seg.r AS rel, type(seg.r) AS rType
      OPTIONAL MATCH (sNode)-[taughtF:TAUGHT]->(tNode)
      OPTIONAL MATCH (tNode)-[taughtB:TAUGHT]->(sNode)
      WITH ns, sNode, tNode, rel, rType, taughtF, taughtB,
           CASE
             WHEN rType = 'TAUGHT' AND taughtF IS NOT NULL THEN sNode
             WHEN rType = 'TAUGHT' AND taughtB IS NOT NULL THEN tNode
             WHEN rType IN ['PREMIERED_ROLE_IN','AUTHORED','COMPOSED','EDITED'] AND 'Person' IN labels(sNode) THEN sNode
             WHEN rType IN ['PREMIERED_ROLE_IN','AUTHORED','COMPOSED','EDITED'] AND 'Person' IN labels(tNode) THEN tNode
             ELSE startNode(rel)
           END AS oStart,
           CASE
             WHEN rType = 'TAUGHT' AND taughtF IS NOT NULL THEN tNode
             WHEN rType = 'TAUGHT' AND taughtB IS NOT NULL THEN sNode
             WHEN rType IN ['PREMIERED_ROLE_IN','AUTHORED','COMPOSED','EDITED'] AND NOT 'Person' IN labels(sNode) AND 'Person' IN labels(tNode) THEN sNode
             WHEN rType IN ['PREMIERED_ROLE_IN','AUTHORED','COMPOSED','EDITED'] AND 'Person' IN labels(sNode) AND NOT 'Person' IN labels(tNode) THEN tNode
             ELSE endNode(rel)
           END AS oEnd
      RETURN collect({ 
        relType: rType, 
        role: rel.role, 
        source: oStart, 
        target: oEnd,
        sourceText: CASE
          WHEN rType IN ['TAUGHT','PARENT','SPOUSE','SIBLING','GRANDPARENT'] THEN coalesce(rel.teacher_rel_source_text, rel.teacher_rel_source, rel.relationship_source, rel.source)
          WHEN rType IN ['PREMIERED_ROLE_IN','WROTE'] THEN coalesce(rel.opera_source_text, rel.relationship_source, rel.source)
          WHEN rType IN ['AUTHORED','EDITED'] THEN coalesce(rel.relationship_source, rel.source)
          ELSE coalesce(rel.relationship_source, rel.source)
        END,
        sourceUrl: CASE
          WHEN rType IN ['TAUGHT','PARENT','SPOUSE','SIBLING','GRANDPARENT'] THEN rel.teacher_rel_source_url
          WHEN rType IN ['PREMIERED_ROLE_IN','WROTE'] THEN rel.opera_source_url
          WHEN rType IN ['AUTHORED','EDITED'] THEN rel.book_source_url
          ELSE NULL
        END,
        sourceInfo: CASE
          WHEN rType IN ['TAUGHT','PARENT','SPOUSE','SIBLING','GRANDPARENT'] THEN coalesce(rel.teacher_rel_source_text, rel.teacher_rel_source, rel.relationship_source, rel.source)
          WHEN rType IN ['PREMIERED_ROLE_IN','WROTE'] THEN coalesce(rel.opera_source_text, rel.relationship_source, rel.source)
          WHEN rType IN ['AUTHORED','EDITED'] THEN coalesce(rel.relationship_source, rel.source)
          ELSE coalesce(rel.relationship_source, rel.source)
        END
      }) AS oriented,
             ns AS pathNodes
    `;

    const result = await session.run(optimizedQuery, { from, to });
    if (result.records.length === 0) {
      console.log('[path/find] no resolution for from/to');
      setCachedPath(from, to, hops, { error: 'No path found' }, true);
      return res.status(404).json({ error: 'No path found' });
    }
    const record = result.records[0];
    const oriented = record.get('oriented');
    const pathNodes = record.get('pathNodes');

    if (!oriented || oriented.length === 0) {
      console.log('[path/find] no path found');
      setCachedPath(from, to, hops, { error: 'No path found' }, true);
      return res.status(404).json({ error: 'No path found' });
    }

    // Collect nodes and links based on oriented segments
    const nodesMap = new Map();
    const links = [];
    const steps = [];

    const getNodeInfo = (node) => {
      const labels = node.labels || [];
      const props = node.properties || {};
      if (labels.includes('Person')) {
        const personId = normalizeIdComponent(props.person_id ?? props.personId ?? props.id);
        const displayName = toPlainString(props.full_name) || toPlainString(props.name) || toPlainString(props.label);
        return {
          id: buildTypedId('person', personId, displayName),
          name: displayName || (personId ? `Person ${personId}` : 'Unknown Person'),
          type: 'person'
        };
      }
      if (labels.includes('Opera')) {
        const operaId = normalizeIdComponent(props.opera_id ?? props.id);
        const displayName = toPlainString(props.opera_name) || toPlainString(props.title) || toPlainString(props.name) || toPlainString(props.label);
        return {
          id: buildTypedId('opera', operaId, displayName),
          name: displayName || (operaId ? `Opera ${operaId}` : 'Unknown Opera'),
          type: 'opera',
          composer: props.composer || null
        };
      }
      if (labels.includes('Book')) {
        const bookId = normalizeIdComponent(props.book_id ?? props.id);
        const displayName = toPlainString(props.title) || toPlainString(props.name) || toPlainString(props.label);
        return {
          id: buildTypedId('book', bookId, displayName),
          name: displayName || (bookId ? `Book ${bookId}` : 'Unknown Book'),
          type: 'book'
        };
      }
      const fallbackId = normalizeIdComponent(props.id ?? props.name) || JSON.stringify(props);
      const fallbackName = toPlainString(props.name) || toPlainString(props.label) || fallbackId || 'Unknown';
      return {
        id: buildTypedId('unknown', fallbackId, fallbackName),
        name: fallbackName,
        type: 'unknown'
      };
    };

    oriented.forEach((seg, idx) => {
      const startInfo = getNodeInfo(seg.source);
      const endInfo = getNodeInfo(seg.target);
      if (!nodesMap.has(startInfo.id)) nodesMap.set(startInfo.id, startInfo);
      if (!nodesMap.has(endInfo.id)) nodesMap.set(endInfo.id, endInfo);
      const relType = seg.relType;
      const sourceText = seg.sourceText || seg.sourceInfo || null;
      const sourceUrl = seg.sourceUrl || null;
      const toFrontType = (t) => {
        switch ((t || '').toUpperCase()) {
          case 'TAUGHT': return 'taught';
          case 'FAMILY': return 'family';
          case 'PREMIERED_ROLE_IN': return 'premiered';
          case 'AUTHORED': return 'authored';
          case 'COMPOSED': return 'composed';
          case 'EDITED': return 'edited';
          default: return (t || '').toLowerCase();
        }
      };
      // Use oriented direction directly from Cypher
      const frontType = toFrontType(relType);
      const emitSource = startInfo;
      const emitTarget = endInfo;
      const link = {
        source: emitSource.id,
        target: emitTarget.id,
        type: frontType,
        label: frontType === 'premiered' ? 'premiered role in' : frontType,
        order: idx
      };
      if (toFrontType(relType) === 'premiered') {
        link.role = seg.role;
      }
      if (sourceText) {
        link.relationshipSourceDisplay = sourceText;
        link.sourceInfo = sourceText;
        link.relationship_source = sourceText;
      }
      if (sourceUrl) {
        link.sourceUrl = sourceUrl;
      }
      const teacherLikeTypes = new Set(['taught', 'family', 'parent', 'grandparent', 'sibling', 'spouse', 'coach', 'coached']);
      if (teacherLikeTypes.has(frontType)) {
        link.teacher_rel_source_text = sourceText || null;
        link.teacher_rel_source_url = sourceUrl || null;
        link.teacher_rel_source = sourceText || null;
      } else if (frontType === 'premiered' || frontType === 'composed') {
        link.opera_source_text = sourceText || null;
        link.opera_source_url = sourceUrl || null;
      }
      links.push(link);

      steps.push({
        order: idx,
        source: emitSource,
        target: emitTarget,
        type: link.type,
        label: link.label,
        role: link.role,
        sourceInfo: sourceText,
        relationshipSourceDisplay: sourceText,
        relationship_source: sourceText,
        sourceUrl
      });
    });

    const payload = {
      nodes: Array.from(nodesMap.values()),
      links,
      steps
    };
    setCachedPath(from, to, hops, payload, false);
    console.log(`[path/find] served in ${Date.now() - startedAt}ms`);
    res.json(payload);
  } catch (error) {
    console.error('Path find error:', error);
    res.status(500).json({ error: 'Failed to find path' });
  } finally {
    await session.close();
  }
});

// Test endpoint for connectivity
app.get('/test', (req, res) => {
  res.json({ message: 'Backend is working!', timestamp: new Date().toISOString() });
});

// Health and readiness endpoints
app.get('/health', (req, res) => {
  res.json({ status: 'ok', uptime: Math.round(process.uptime()), timestamp: new Date().toISOString() });
});
app.get('/ready', async (req, res) => {
  try {
    // Check MySQL
    const pool = await getMysqlPool();
    await pool.query('SELECT 1');
    // Check Neo4j
    try { await driver.verifyConnectivity(); } catch (_) {
      // Fallback lightweight query
      const session = driver.session();
      try { await session.run('RETURN 1'); } finally { await session.close(); }
    }
    res.json({ ready: true });
  } catch (err) {
    console.error('[ready] not ready:', err?.message);
    res.status(503).json({ ready: false, error: 'Dependency check failed' });
  }
});

// Post-auth bridge: after Auth0 callback, redirect back to frontend with an app token (or pending ToS)
app.get('/post-auth', async (req, res) => {
  try {
    const rawNext = (req.query?.next || '').toString();
    const nextUrl = resolveAllowedReturnTo(rawNext);
    if (!req.oidc || !req.oidc.isAuthenticated()) {
      const loginUrl = `${BASE_URL}/login?returnTo=${encodeURIComponent(nextUrl)}`;
      return res.redirect(loginUrl);
    }

    const email = (req.oidc.user?.email || '').trim().toLowerCase();
    if (!email) {
      return res.redirect(nextUrl);
    }

    const claimTosVersion = Number(req.oidc.user?.[AUTH0_TOS_CLAIM] || 0);
    const metadataTosVersion = Number((req.oidc.user?.app_metadata?.tos_version) || 0);
    const effectiveTosVersion = Math.max(claimTosVersion, metadataTosVersion);

    // Ensure we have a local record and a stable public_user_id; mirror to Auth0 app_metadata when possible
    try {
      await ensureUserRecordForEmail(email);
      await getOrCreatePublicIdForEmail(email);
    } catch (_) {}

    if (TOS_ENFORCEMENT_MODE === 'auth0_only' && effectiveTosVersion < ACTIVE_TOS_VERSION) {
      // First, consult Auth0 app_metadata via Management API (if configured)
      if (AUTH0_MGMT_CLIENT_ID && AUTH0_MGMT_CLIENT_SECRET) {
        try {
          const u = await mgmtFindUserByEmail(email);
          const m = u && u.app_metadata ? u.app_metadata : {};
          const v = Number(m.tos_version || 0);
          if (Number.isFinite(v) && v >= ACTIVE_TOS_VERSION) {
            const appToken = generateAuthToken(email);
            const hashOk = `#authToken=${encodeURIComponent(appToken)}`;
            const redirectOk = `${nextUrl.replace(/#.*$/, '')}${hashOk}`;
            return res.redirect(redirectOk);
          }
        } catch (_) {}
      }
      // If user previously accepted ToS via in‑app modal, trust cookie and skip prompt
      try {
        const v = getCookie(req, 'cmg_tos_v');
        if (v) {
          try {
            const payload = JSON.parse(Buffer.from(v, 'base64').toString('utf8'));
            if (payload && payload.email && payload.email.toLowerCase() === email && Number(payload.v || 0) >= ACTIVE_TOS_VERSION) {
              const appToken = generateAuthToken(email);
              const hashOk = `#authToken=${encodeURIComponent(appToken)}`;
              const redirectOk = `${nextUrl.replace(/#.*$/, '')}${hashOk}`;
              return res.redirect(redirectOk);
            }
          } catch (_) {}
        }
      } catch (_) {}
      // No in-memory fallback; rely on cookie or Auth0 app_metadata only
      const pendingToken = generatePendingTosToken(email);
      const hash = `#pendingTosToken=${encodeURIComponent(pendingToken)}&email=${encodeURIComponent(email)}`;
      const redirect = `${nextUrl.replace(/#.*$/, '')}${hash}`;
      return res.redirect(redirect);
    }

    const appToken = generateAuthToken(email);
    const hash = `#authToken=${encodeURIComponent(appToken)}`;
    const redirect = `${nextUrl.replace(/#.*$/, '')}${hash}`;
    return res.redirect(redirect);
  } catch (err) {
    console.error('[post-auth] error:', err?.message || err);
    try { return res.redirect(CLIENT_BASE_URL || BASE_URL); } catch (_) { return res.status(500).end(); }
  }
});

// View snapshot storage (file-based per-user)
const SAMPLE_VIEW_DIRS = [
  path.resolve(__dirname, 'data', 'views', 'test%40example.com'),
  path.resolve(__dirname, 'sample-views')
].filter((dir) => fs.existsSync(dir));

const readSnapshotFromDir = async (dir, token) => {
  const file = path.join(dir, `${token}.json`);
  try {
    const raw = await fs.promises.readFile(file, 'utf8');
    return JSON.parse(raw);
  } catch (err) {
    if (err && err.code === 'ENOENT') return null;
    throw err;
  }
};

// Save a snapshot and return a token
app.post('/views', authenticateToken, async (req, res) => {
  if (!DB_WRITES_ENABLED) {
    return res.status(503).json({ error: 'Saving views is temporarily disabled (read-only mode).' });
  }
  await new Promise(resolve => viewsWriteLimiter(req, res, resolve));
  try {
    const { snapshot, label = '' } = req.body || {};
    if (!snapshot || typeof snapshot !== 'object') {
      return res.status(400).json({ error: 'snapshot required' });
    }
    // Guard against oversized snapshots to keep DB fast and requests light
    try {
      const json = JSON.stringify(snapshot);
      const bytes = Buffer.byteLength(json, 'utf8');
      if (bytes > MAX_SNAPSHOT_BYTES) {
        return res.status(413).json({
          error: `Snapshot too large (${bytes.toLocaleString()} bytes). Please reduce nodes/links or filters and try again.`
        });
      }
    } catch (_) {}

    const token = crypto.randomUUID();
    const createdAt = new Date();
    const pool = await getMysqlPool();
    await ensureSavedViewsTable(pool);
    // Resolve or create the stable public ID for this user
    const publicId = await getOrCreatePublicIdForEmail(req.user.email || '');
    if (!publicId) {
      return res.status(500).json({ error: 'Could not resolve user identifier for saving view' });
    }
    await pool.query(
      'INSERT INTO saved_views (user_public_id, token, label, snapshot, created_at) VALUES (?, ?, ?, ?, ?)',
      [publicId, token, label, JSON.stringify(snapshot), createdAt]
    );

    return res.json({ token, label, createdAt: createdAt.toISOString() });
  } catch (err) {
    console.error('Save view error:', err);
    return res.status(500).json({ error: 'Failed to save view' });
  }
});

// List snapshots for current user
app.get('/views', authenticateToken, async (req, res) => {
  await new Promise(resolve => viewsReadLimiter(req, res, resolve));
  try {
    const pool = await getMysqlPool();
    await ensureSavedViewsTable(pool);
    const publicId = await getOrCreatePublicIdForEmail(req.user.email || '');
    if (!publicId) {
      return res.status(500).json({ error: 'Could not resolve user identifier for listing views' });
    }
    const [rows] = await pool.query(
      'SELECT token, label, created_at FROM saved_views WHERE user_public_id = ? ORDER BY created_at DESC',
      [publicId]
    );
    const views = rows.map((row) => ({
      token: row.token,
      label: row.label || '',
      createdAt: row.created_at ? new Date(row.created_at).toISOString() : null
    }));
    return res.json({ views });
  } catch (err) {
    console.error('List views error:', err);
    return res.status(500).json({ error: 'Failed to list views' });
  }
});

// Load a snapshot by token (user-scoped)
app.get('/views/:token', authenticateToken, async (req, res) => {
  await new Promise(resolve => viewsReadLimiter(req, res, resolve));
  try {
    const { token } = req.params || {};
    if (!token) return res.status(400).json({ error: 'token required' });

    let data = null;

    const pool = await getMysqlPool();
    await ensureSavedViewsTable(pool);
    const [rows] = await pool.query(
      'SELECT token, label, created_at, snapshot, user_public_id FROM saved_views WHERE token = ?',
      [token]
    );
    if (rows.length > 0) {
      const row = rows[0];
      const currentPublicId = await getOrCreatePublicIdForEmail(req.user.email || '');
      if (!currentPublicId || row.user_public_id !== currentPublicId) {
        return res.status(403).json({ error: 'Forbidden' });
      }
      let snapshotPayload = row.snapshot;
      if (snapshotPayload && typeof snapshotPayload === 'string') {
        try {
          snapshotPayload = JSON.parse(snapshotPayload);
        } catch (err) {
          console.warn('Failed to parse snapshot JSON from MySQL:', err?.message);
        }
      }
      data = {
        token: row.token,
        label: row.label || '',
        createdAt: row.created_at ? new Date(row.created_at).toISOString() : null,
        snapshot: snapshotPayload
      };
    }

    if (!data) {
      for (const fallbackDir of SAMPLE_VIEW_DIRS) {
        try {
          const fallback = await readSnapshotFromDir(fallbackDir, token);
          if (fallback) {
            data = fallback;
            break;
          }
        } catch (err) {
          if (!(err && err.code === 'ENOENT')) {
            throw err;
          }
        }
      }
    }

    if (!data) {
      return res.status(404).json({ error: 'Not found' });
    }

    return res.json({
      token: data.token,
      label: data.label || '',
      createdAt: data.createdAt || null,
      snapshot: data.snapshot
    });
  } catch (err) {
    console.error('Load view error:', err);
    return res.status(500).json({ error: 'Failed to load view' });
  }
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({ error: 'Something went wrong!' });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({ error: 'Endpoint not found' });
});

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down gracefully...');
  if (neo4jKeepAliveInterval) clearInterval(neo4jKeepAliveInterval);
  await driver.close();
  process.exit(0);
});

app.listen(PORT, () => {
  console.log(`💥 Server running on http://0.0.0.0:${PORT}`);
  console.log(`💥 Environment: ${process.env.NODE_ENV}`);
  startNeo4jKeepAlive();
});
// Helper to parse cookies safely without extra deps
const getCookie = (req, name) => {
  try {
    const raw = req.headers?.cookie || '';
    const parts = raw.split(/;\s*/);
    for (const p of parts) {
      const idx = p.indexOf('=');
      if (idx === -1) continue;
      const k = p.slice(0, idx).trim();
      if (k === name) return decodeURIComponent(p.slice(idx + 1));
    }
  } catch (_) {}
  return null;
};
const setCookie = (res, name, value, opts = {}) => {
  const {
    maxAgeSec = 365 * 24 * 60 * 60,
    path = '/',
    sameSite = AUTH0_BASE_IS_HTTPS ? 'None' : 'Lax',
    secure = AUTH0_BASE_IS_HTTPS,
    httpOnly = true,
    domain = AUTH0_COOKIE_DOMAIN || undefined
  } = opts;
  const parts = [`${name}=${encodeURIComponent(value)}`, `Path=${path}`, `Max-Age=${Math.max(1, maxAgeSec)}`];
  if (domain) parts.push(`Domain=${domain}`);
  if (sameSite) parts.push(`SameSite=${sameSite}`);
  if (secure) parts.push('Secure');
  if (httpOnly) parts.push('HttpOnly');
  try {
    // Append to support multiple Set-Cookie headers
    res.append('Set-Cookie', parts.join('; '));
  } catch (_) {
    res.setHeader('Set-Cookie', parts.join('; '));
  }
};

// In-memory fallback for ToS acceptance (survives until server restarts)
// Removed in-memory ToS fallback to avoid cross-session confusion
// const tosLocalAcceptance = new Map(); // key: email -> { v, at }

// Management API helpers (Node 18+ has global fetch)
let mgmtTokenCache = { token: null, exp: 0 };
const getMgmtToken = async () => {
  try {
    if (!AUTH0_MGMT_CLIENT_ID || !AUTH0_MGMT_CLIENT_SECRET || !AUTH0_ISSUER_BASE_URL) return null;
    const now = Date.now();
    if (mgmtTokenCache.token && now < mgmtTokenCache.exp - 60_000) return mgmtTokenCache.token;
    const resp = await fetch(`${AUTH0_ISSUER_BASE_URL}/oauth/token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        grant_type: 'client_credentials',
        client_id: AUTH0_MGMT_CLIENT_ID,
        client_secret: AUTH0_MGMT_CLIENT_SECRET,
        audience: AUTH0_MGMT_AUDIENCE
      })
    });
    if (!resp.ok) return null;
    const data = await resp.json();
    const token = data.access_token;
    const ttl = Number(data.expires_in || 3600) * 1000;
    mgmtTokenCache = { token, exp: now + ttl };
    return token || null;
  } catch (_) { return null; }
};

const mgmtFindUserByEmail = async (email) => {
  const token = await getMgmtToken();
  if (!token) return null;
  const resp = await fetch(`${AUTH0_MGMT_AUDIENCE}users-by-email?email=${encodeURIComponent(email)}`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  if (!resp.ok) return null;
  const arr = await resp.json().catch(() => []);
  return Array.isArray(arr) && arr.length > 0 ? arr[0] : null;
};

const mgmtUpdateUserAppMetadata = async (userId, meta) => {
  const token = await getMgmtToken();
  if (!token || !userId) return false;
  const resp = await fetch(`${AUTH0_MGMT_AUDIENCE}users/${encodeURIComponent(userId)}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ app_metadata: meta })
  });
  return resp.ok;
};
