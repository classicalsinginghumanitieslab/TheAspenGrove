// backend/routes/auth0Migration.js
import express from 'express';
import bcrypt from 'bcryptjs';

export default function createAuth0MigrationRouter({ getMysqlPool, expectedToken, activeTosVersion = 0, dbWritesEnabled = true }) {
  const router = express.Router();

  const isAuthorized = (req) => {
    const authHeader = req.headers['authorization'] || '';
    return Boolean(expectedToken && authHeader === `Bearer ${expectedToken}`);
  };

  const ensureAuthorized = (req, res) => {
    if (!isAuthorized(req)) {
      res.status(403).json({ error: 'Unauthorized' });
      return false;
    }
    return true;
  };

  router.post('/auth0-migration/verify', async (req, res) => {
    try {
      if (!ensureAuthorized(req, res)) return;

      const { email, password } = req.body || {};
      if (typeof email !== 'string' || typeof password !== 'string' || !email || !password) {
        return res.status(400).json({ error: 'Email and password required' });
      }

      const pool = await getMysqlPool();
      const [rows] = await pool.query(
        'SELECT id, email, password_hash, created_at, tos_version, tos_accepted_at, migrated_to_auth0, migrated_at FROM users WHERE email = ? LIMIT 1',
        [email.toLowerCase()]
      );
      if (rows.length === 0) {
        return res.status(401).json({ error: 'Invalid credentials' });
      }

      const user = rows[0];
      const ok = await bcrypt.compare(password, user.password_hash);
      if (!ok) {
        return res.status(401).json({ error: 'Invalid credentials' });
      }

      // Return fields for Auth0 migration: identity + app_metadata
      const tosVersion = Number(user.tos_version || 0);
      const tosAcceptedAt = user.tos_accepted_at ? new Date(user.tos_accepted_at).toISOString() : null;
      const createdAt = user.created_at ? new Date(user.created_at).toISOString() : null;
      const migratedAtIso = new Date().toISOString();
      res.json({
        user_id: String(user.id),
        email: user.email,
        email_verified: true,
        app_metadata: {
          legacy_user_id: String(user.id),
          legacy_created_at: createdAt,
          tos_version: tosVersion,
          tos_accepted_at: tosAcceptedAt,
          migrated_at: migratedAtIso
        }
      });

      // Optional: best-effort audit (ignore if columns don’t exist)
      try {
        await pool.query(
          'UPDATE users SET migrated_to_auth0 = 1, migrated_at = NOW() WHERE id = ?',
          [user.id]
        );
      } catch (_) {}
    } catch (err) {
      console.error('[auth0-migration] error:', err);
      return res.status(500).json({ error: 'Internal server error' });
    }
  });

  // Minimal endpoint for Auth0 Custom DB "Get User" script
  // Input: { email }
  // Output:
  //   200 { user_id, email } if found
  //   404 { found: false } if not found
  router.post('/auth0-migration/get-user', async (req, res) => {
    try {
      if (!ensureAuthorized(req, res)) return;

      const { email } = req.body || {};
      const normalized = (email || '').trim().toLowerCase();
      if (!normalized) {
        return res.status(400).json({ error: 'Email required' });
      }

      const pool = await getMysqlPool();
      const [rows] = await pool.query(
        'SELECT id, email, created_at, tos_version, tos_accepted_at FROM users WHERE email = ? LIMIT 1',
        [normalized]
      );
      if (rows.length === 0) {
        return res.status(404).json({ found: false });
      }

      const user = rows[0];
      const tosVersion = Number(user.tos_version || 0);
      const tosAcceptedAt = user.tos_accepted_at ? new Date(user.tos_accepted_at).toISOString() : null;
      const createdAt = user.created_at ? new Date(user.created_at).toISOString() : null;
      return res.json({
        user_id: String(user.id),
        email: user.email,
        email_verified: true,
        app_metadata: {
          legacy_user_id: String(user.id),
          legacy_created_at: createdAt,
          tos_version: tosVersion,
          tos_accepted_at: tosAcceptedAt
        }
      });
    } catch (err) {
      console.error('[auth0-migration] get-user error:', err);
      return res.status(500).json({ error: 'Failed to look up user' });
    }
  });

  router.post('/auth0-migration/create', async (req, res) => {
    try {
      if (!dbWritesEnabled) {
        return res.status(503).json({ error: 'User creation is managed by Auth0. Legacy DB writes are disabled.' });
      }
      if (!ensureAuthorized(req, res)) return;

      const { email, password, acceptedDisclaimer } = req.body || {};
      if (typeof email !== 'string' || typeof password !== 'string' || !email || !password) {
        return res.status(400).json({ error: 'Email and password required' });
      }

      const normalizedEmail = email.trim().toLowerCase();
      if (!normalizedEmail) {
        return res.status(400).json({ error: 'Email is required' });
      }

      const pool = await getMysqlPool();
      const [existing] = await pool.query('SELECT id FROM users WHERE email = ? LIMIT 1', [normalizedEmail]);
      if (existing.length > 0) {
        return res.status(409).json({ error: 'User already exists' });
      }

      const hashedPassword = await bcrypt.hash(password, 10);
      const tosAccepted = acceptedDisclaimer === true || acceptedDisclaimer === 'true' || acceptedDisclaimer === 1;
      const tosVersion = tosAccepted ? activeTosVersion : 0;
      const tosAcceptedAt = tosAccepted ? new Date() : null;

      const [result] = await pool.query(
        'INSERT INTO users (email, password_hash, tos_version, tos_accepted_at) VALUES (?, ?, ?, ?)',
        [normalizedEmail, hashedPassword, tosVersion, tosAcceptedAt]
      );

      return res.status(201).json({
        user_id: String(result.insertId),
        email: normalizedEmail,
        email_verified: false,
        app_metadata: {
          legacy_user_id: String(result.insertId),
          legacy_created_at: new Date().toISOString(),
          tos_version: tosVersion,
          tos_accepted_at: tosAcceptedAt ? new Date(tosAcceptedAt).toISOString() : null
        }
      });
    } catch (err) {
      console.error('[auth0-migration] create error:', err);
      return res.status(500).json({ error: 'Failed to create user' });
    }
  });

  return router;
}
