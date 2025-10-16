import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import rateLimit from 'express-rate-limit';
import neo4j from 'neo4j-driver';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import crypto from 'crypto';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3001;
const HOST = process.env.HOST || '0.0.0.0';
const JWT_SECRET = process.env.JWT_SECRET || 'dev-secret';
const JWT_EXPIRES_IN = process.env.JWT_EXPIRES_IN || '7d';

// Security middleware
app.set('trust proxy', 1);
app.use(helmet());
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

// Allow typical private-LAN hostnames like http://192.168.x.x:PORT, http://10.x.x.x:PORT
const privateLan = [/^http:\/\/(?:192\.168|10\.\d{1,3}|172\.(?:1[6-9]|2\d|3[0-1]))\.\d{1,3}:\d+$/];
const cloudflareTunnel = /^https?:\/\/[a-z0-9-]+\.trycloudflare\.com$/;

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
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
};

app.use(cors(corsOptions));
app.options('*', cors(corsOptions));

// Rate limiting
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100 // limit each IP to 100 requests per windowMs
});
app.use(limiter);

app.use(express.json({ limit: '10mb' }));

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

  jwt.verify(token, JWT_SECRET, (err, user) => {
    if (err) {
      console.error('JWT verification error:', err);
      return res.status(403).json({ error: 'Invalid or expired token' });
    }
    req.user = user;
    next();
  });
};

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'OK', timestamp: new Date().toISOString() });
});

// Authentication endpoints
app.post('/auth/register', async (req, res) => {
  const session = driver.session();
  try {
    const { email, password } = req.body;
    
    if (!email || !password) {
      return res.status(400).json({ error: 'Email and password required' });
    }

    // Check if user already exists
    const existingUser = await session.run(
      'MATCH (u:User {email: $email}) RETURN u',
      { email }
    );

    if (existingUser.records.length > 0) {
      return res.status(400).json({ error: 'User already exists' });
    }

    // Hash password
    const hashedPassword = await bcrypt.hash(password, 10);

    // Create user
    await session.run(
      'CREATE (u:User {email: $email, password: $password, createdAt: datetime()})',
      { email, password: hashedPassword }
    );

    // Generate JWT token
    const token = jwt.sign(
      { email },
      process.env.JWT_SECRET,
      { expiresIn: process.env.JWT_EXPIRES_IN }
    );

    res.status(201).json({ token, email });
  } catch (error) {
    console.error('Registration error:', error);
    res.status(500).json({ error: 'Internal server error' });
  } finally {
    await session.close();
  }
});

app.post('/auth/login', async (req, res) => {
  // TEST USER OVERRIDE: Always allow test@example.com / password123
  if (req.body.email === 'test@example.com' && req.body.password === 'password123') {
    const token = jwt.sign({ email: 'test@example.com' }, JWT_SECRET, { expiresIn: JWT_EXPIRES_IN });
    return res.json({ token, email: 'test@example.com' });
  }
  const session = driver.session();
  try {
    const { email, password } = req.body;
    
    if (!email || !password) {
      return res.status(400).json({ error: 'Email and password required' });
    }

    // Find user
    const result = await session.run(
      'MATCH (u:User {email: $email}) RETURN u',
      { email }
    );

    if (result.records.length === 0) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    const user = result.records[0].get('u').properties;
    
    // Verify password
    const isValidPassword = await bcrypt.compare(password, user.password);
    if (!isValidPassword) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    // Generate JWT token
    const token = jwt.sign({ email }, JWT_SECRET, { expiresIn: JWT_EXPIRES_IN });

    res.json({ token, email });
  } catch (error) {
    console.error('Login error:', error);
    res.status(500).json({ error: 'Internal server error' });
  } finally {
    await session.close();
  }
});

// Add this debug endpoint before the existing endpoints
app.post('/debug/corrupted-premieres', authenticateToken, async (req, res) => {
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
app.post('/debug/eric-tappy-roles', authenticateToken, async (req, res) => {
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
app.post('/debug/fix-null-premiere', authenticateToken, async (req, res) => {
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
    const { query, limit = 20 } = req.body;
    console.log('About to run Neo4j query');
    
    // Manual diacritical character replacement for German umlauts
    const cleanQuery = (query || '')
      .toLowerCase()
      .replace(/ä/g, 'a')
      .replace(/ö/g, 'o') 
      .replace(/ü/g, 'u')
      .replace(/ß/g, 'ss');
    
    const result = await session.run(
      `MATCH (s:Person) 
       WHERE apoc.text.clean(toLower(s.full_name)) CONTAINS apoc.text.clean(toLower($query))
          OR toLower(s.full_name) CONTAINS toLower($query)
          OR apoc.text.replace(apoc.text.replace(apoc.text.replace(apoc.text.replace(toLower(s.full_name), 'ä', 'a'), 'ö', 'o'), 'ü', 'u'), 'ß', 'ss') CONTAINS $cleanQuery
       RETURN s.full_name as name, s as properties
       ORDER BY s.full_name
       LIMIT $limit`,
      { query: query || '', cleanQuery, limit: neo4j.int(limit) }
    );
    console.log('Neo4j query complete');
    const singers = result.records.map(record => ({
      name: record.get('name'),
      properties: record.get('properties').properties
    }));
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
    const { query, limit = 20 } = req.body;
    
    // Manual diacritical character replacement for German umlauts
    const cleanQuery = (query || '')
      .toLowerCase()
      .replace(/ä/g, 'a')
      .replace(/ö/g, 'o') 
      .replace(/ü/g, 'u')
      .replace(/ß/g, 'ss');
    
    const result = await session.run(
      `MATCH (o:Opera) 
       WHERE apoc.text.clean(toLower(o.opera_name)) CONTAINS apoc.text.clean(toLower($query))
          OR toLower(o.opera_name) CONTAINS toLower($query)
          OR apoc.text.replace(apoc.text.replace(apoc.text.replace(apoc.text.replace(toLower(o.opera_name), 'ä', 'a'), 'ö', 'o'), 'ü', 'u'), 'ß', 'ss') CONTAINS $cleanQuery
       RETURN o as properties
       ORDER BY o.opera_name
       LIMIT $limit`,
      { query: query || '', cleanQuery, limit: neo4j.int(limit) }
    );

    const operas = result.records.map(record => ({
      properties: record.get('properties').properties
    }));

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
    const { query, limit = 20 } = req.body;
    
    // Manual diacritical character replacement for German umlauts
    const cleanQuery = (query || '')
      .toLowerCase()
      .replace(/ä/g, 'a')
      .replace(/ö/g, 'o') 
      .replace(/ü/g, 'u')
      .replace(/ß/g, 'ss');
    
    const result = await session.run(
      `MATCH (b:Book) 
       WHERE apoc.text.clean(toLower(b.title)) CONTAINS apoc.text.clean(toLower($query))
          OR toLower(b.title) CONTAINS toLower($query)
          OR apoc.text.replace(apoc.text.replace(apoc.text.replace(apoc.text.replace(toLower(b.title), 'ä', 'a'), 'ö', 'o'), 'ü', 'u'), 'ß', 'ss') CONTAINS $cleanQuery
       RETURN b as properties
       ORDER BY b.title
       LIMIT $limit`,
      { query: query || '', cleanQuery, limit: neo4j.int(limit) }
    );

    const books = result.records.map(record => ({
      properties: record.get('properties').properties
    }));

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

    const center = singerResult.records[0].get('s').properties;

    // Get teachers with relationship source
    const teachersResult = await session.run(
      `MATCH (s:Person {full_name: $name})<-[r:TAUGHT]-(t:Person)
       RETURN t AS teacher_node, r AS teacher_relationship`,
      { name: singerName }
    );
    const teachers = teachersResult.records.map(r => {
      const teacherNodeValue = r.get('teacher_node');
      const teacherRelValue = r.get('teacher_relationship');
      const teacherNode = teacherNodeValue ? teacherNodeValue.properties || {} : {};
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
      const studentNode = studentNodeValue ? studentNodeValue.properties || {} : {};
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
      const f = r.get('f').properties;
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
      'MATCH (s:Person {full_name: $name})-[r:PREMIERED_ROLE_IN]->(o:Opera) RETURN o.opera_name as opera_name, r.role as role, r.opera_source_text as opera_source_text, r.opera_source_url as opera_source_url, r.source as legacy_source',
      { name: singerName }
    );
    const operas = operasResult.records.map(r => {
      const sourceTextRaw = r.get('opera_source_text');
      const sourceUrlRaw = r.get('opera_source_url');
      const legacySourceRaw = r.get('legacy_source');
      const resolvedSourceText = pickRelationshipSourceValue(sourceTextRaw, legacySourceRaw);
      const resolvedSourceUrl = normalizeRelationshipSourceValue(sourceUrlRaw);
      return {
        opera_name: r.get('opera_name'),
        role: r.get('role'),
        source: resolvedSourceText || 'Unknown',
        opera_source_text: resolvedSourceText || null,
        opera_source_url: resolvedSourceUrl || null
      };
    });

    // Get specific roles premiered (for the new Roles premiered card)
    const premieredRolesResult = await session.run(
      'MATCH (s:Person {full_name: $name})-[r:PREMIERED_ROLE_IN]->(o:Opera) RETURN o.opera_name as opera_name, r.role as role, r.opera_source_text as opera_source_text, r.opera_source_url as opera_source_url, r.source as legacy_source',
      { name: singerName }
    );
    const premieredRoles = premieredRolesResult.records.map(r => {
      const sourceTextRaw = r.get('opera_source_text');
      const sourceUrlRaw = r.get('opera_source_url');
      const legacySourceRaw = r.get('legacy_source');
      const resolvedSourceText = pickRelationshipSourceValue(sourceTextRaw, legacySourceRaw);
      const resolvedSourceUrl = normalizeRelationshipSourceValue(sourceUrlRaw);
      return {
        opera_name: r.get('opera_name'),
        role: r.get('role'),
        source: resolvedSourceText || 'Unknown',
        opera_source_text: resolvedSourceText || null,
        opera_source_url: resolvedSourceUrl || null
      };
    });

    const booksResult = await session.run(
      `MATCH (s:Person {full_name: $name})-[r:AUTHORED]->(b:Book)
       RETURN b.title as title, r AS relationship`,
      { name: singerName }
    );
    const books = booksResult.records.map(r => ({
      title: r.get('title')
    }));

    const composedOperasResult = await session.run(
      `MATCH (s:Person {full_name: $name})-[r:COMPOSED|WROTE]->(o:Opera)
       RETURN o.title as title, r AS relationship`,
      { name: singerName }
    );
    const composedOperasRaw = composedOperasResult.records.map(r => {
      const relationshipValue = r.get('relationship');
      const relationshipProps = relationshipValue ? relationshipValue.properties || {} : {};
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
      return {
        title: r.get('title'),
        source: sourceText || 'Unknown',
        opera_source_text: sourceText || null,
        opera_source_url: sourceUrl || null
      };
    });
    const composedOperas = [];
    const seenComposedTitles = new Set();
    for (const opera of composedOperasRaw) {
      const key = opera.title || '';
      if (seenComposedTitles.has(key)) continue;
      seenComposedTitles.add(key);
      composedOperas.push(opera);
    }

    res.json({
      center,
      teachers,
      students,
      family,
      premieredRoles,
      works: {
        operas,
        books,
        composedOperas
      }
    });
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
  const session = driver.session();
  try {
    const { operaName } = req.body;
    
    if (!operaName) {
      return res.status(400).json({ error: 'Opera name required' });
    }

    // Get opera details
    const operaResult = await session.run(
      'MATCH (o:Opera {opera_name: $opera_name}) RETURN o',
      { opera_name: operaName }
    );

    if (operaResult.records.length === 0) {
      return res.status(404).json({ error: 'Opera not found' });
    }

    const opera = operaResult.records[0].get('o').properties;

    // Try to resolve composer from relationships if not present as a property
    let composerName = opera.composer || null;
    try {
      const composerResult = await session.run(
        `MATCH (c:Person)-[r]->(o:Opera {opera_name: $opera_name})
         WHERE type(r) IN ['COMPOSED','WROTE']
         RETURN c.full_name AS composer
         LIMIT 1`,
        { opera_name: operaName }
      );
      if (composerResult.records.length > 0) {
        composerName = composerResult.records[0].get('composer') || composerName;
      }
    } catch (_) {}

    // Get premiered roles
    const rolesResult = await session.run(
      `MATCH (s:Person)-[r:PREMIERED_ROLE_IN]->(o:Opera {opera_name: $opera_name})
       RETURN s.full_name as singer, r.role as role, s.voice_type as voice_type,
         s.voice_type_source as voice_type_source, s.spelling_source as spelling_source, s.dates_source as dates_source, s.birthplace_source as birthplace_source, s.image_source as image_source, s.underrepresented_source as underrepresented_source,
         r.opera_source_text as opera_source_text, r.opera_source_url as opera_source_url, r.source as relationship_source`,
      { opera_name: operaName }
    );
    const premieredRoles = rolesResult.records.map(r => {
      const sourceText = pickRelationshipSourceValue(r.get('opera_source_text'), r.get('relationship_source'));
      const sourceUrl = normalizeRelationshipSourceValue(r.get('opera_source_url'));
      return {
        singer: r.get('singer'),
        role: r.get('role'),
        voice_type: r.get('voice_type'),
        source: sourceText || 'Unknown',
        opera_source_text: sourceText || null,
        opera_source_url: sourceUrl || null
      };
    });

    // Get composers via WROTE relationship (source stored on relationship)
    const wroteResult = await session.run(
      `MATCH (c:Person)-[w:WROTE]->(o:Opera {opera_name: $opera_name})
       RETURN c.full_name as composer, w.opera_source_text as opera_source_text, w.opera_source_url as opera_source_url, w.source as relationship_source
       ORDER BY composer`,
      { opera_name: operaName }
    );
    const wrote = wroteResult.records.map(r => {
      const sourceText = pickRelationshipSourceValue(r.get('opera_source_text'), r.get('relationship_source'));
      const sourceUrl = normalizeRelationshipSourceValue(r.get('opera_source_url'));
      return {
        composer: r.get('composer'),
        source: sourceText || 'Unknown',
        opera_source_text: sourceText || null,
        opera_source_url: sourceUrl || null
      };
    });

    res.json({
      opera: {
        opera_name: opera.opera_name,
        composer: composerName,
        premiere_year: opera.premiere_year
      },
      premieredRoles,
      wrote
    });
  } catch (error) {
    console.error('Opera details error:', error);
    res.status(500).json({ error: 'Failed to fetch opera details' });
  } finally {
    await session.close();
  }
});

app.post('/book/details', authenticateToken, async (req, res) => {
  const session = driver.session();
  try {
    const { bookTitle } = req.body;
    
    if (!bookTitle) {
      return res.status(400).json({ error: 'Book title required' });
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

    res.json({
      book: {
        title: book.title,
        normalized_title: book.normalized_title,
        type: book.type,
        link: book.link,
        book_id: book.book_id
      },
      authors,
      editors
    });
  } catch (error) {
    console.error('Book details error:', error);
    res.status(500).json({ error: 'Failed to fetch book details' });
  } finally {
    await session.close();
  }
});

// Path finding between two people (demo)
app.post('/path/find', authenticateToken, async (req, res) => {
  const session = driver.session();
  const startedAt = Date.now();
  try {
    const { from, to, maxHops } = req.body;
    if (!from || !to) {
      return res.status(400).json({ error: 'from and to (full_name) are required' });
    }
    const hops = Math.min(Math.max(parseInt(maxHops || 8, 10), 1), 12);
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
        return { id: props.full_name, name: props.full_name, type: 'person' };
      }
      if (labels.includes('Opera')) {
        return { id: props.opera_name, name: props.opera_name, type: 'opera', composer: props.composer };
      }
      if (labels.includes('Book')) {
        return { id: props.title, name: props.title, type: 'book' };
      }
      return { id: props.id || props.name || JSON.stringify(props), name: props.name || props.id || 'Unknown', type: 'unknown' };
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
      if (frontType === 'taught' || frontType === 'family') {
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

// View snapshot storage (file-based per-user)
const VIEWS_DIR = path.resolve(__dirname, 'data', 'views');
const ensureDir = async (dir) => {
  await fs.promises.mkdir(dir, { recursive: true });
};
const userDir = async (email) => {
  const safe = encodeURIComponent(email || 'unknown');
  const dir = path.join(VIEWS_DIR, safe);
  await ensureDir(dir);
  return dir;
};

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
  try {
    const { snapshot, label = '' } = req.body || {};
    if (!snapshot || typeof snapshot !== 'object') {
      return res.status(400).json({ error: 'snapshot required' });
    }
    const token = crypto.randomUUID();
    const createdAt = new Date().toISOString();
    const payload = { token, user: req.user.email, label, createdAt, snapshot };
    const dir = await userDir(req.user.email);
    const file = path.join(dir, `${token}.json`);
    await fs.promises.writeFile(file, JSON.stringify(payload, null, 2), 'utf8');
    return res.json({ token, label, createdAt });
  } catch (err) {
    console.error('Save view error:', err);
    return res.status(500).json({ error: 'Failed to save view' });
  }
});

// List snapshots for current user
app.get('/views', authenticateToken, async (req, res) => {
  try {
    const dir = await userDir(req.user.email);
    const files = await fs.promises.readdir(dir).catch(() => []);
    const items = [];
    for (const f of files) {
      if (!f.endsWith('.json')) continue;
      const full = path.join(dir, f);
      try {
        const data = JSON.parse(await fs.promises.readFile(full, 'utf8'));
        items.push({ token: data.token, label: data.label || '', createdAt: data.createdAt || null });
      } catch (_) {}
    }
    items.sort((a, b) => String(b.createdAt || '').localeCompare(String(a.createdAt || '')));
    return res.json({ views: items });
  } catch (err) {
    console.error('List views error:', err);
    return res.status(500).json({ error: 'Failed to list views' });
  }
});

// Load a snapshot by token (user-scoped)
app.get('/views/:token', authenticateToken, async (req, res) => {
  try {
    const { token } = req.params || {};
    if (!token) return res.status(400).json({ error: 'token required' });

    let data = null;

    try {
      const dir = await userDir(req.user.email);
      data = await readSnapshotFromDir(dir, token);
      if (data && data.user && data.user !== req.user.email) {
        return res.status(403).json({ error: 'Forbidden' });
      }
    } catch (err) {
      if (!(err && err.code === 'ENOENT')) {
        throw err;
      }
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
