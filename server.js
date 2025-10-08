// backend/server.js - Complete Enhanced Version
const express = require('express');
const neo4j = require('neo4j-driver');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcrypt');
const rateLimit = require('express-rate-limit');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
require('dotenv').config();

const app = express();

// remove diacritics
const removeDiacritics = (str) => {
  if (!str) return '';
  return str.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
};

// Check if APOC is available
const checkAPOCAvailability = async () => {
  const session = driver.session();
  try {
    await session.run("RETURN apoc.version() as version");
    await session.close();
    return true;
  } catch (error) {
    await session.close();
    console.log('APOC not available, using JavaScript fallback for diacritic removal');
    return false;
  }
};

// Middleware
const allowedOrigins = (process.env.CLIENT_ORIGINS && process.env.CLIENT_ORIGINS.split(',').map(origin => origin.trim())) || [
  'http://localhost:3000',
  'http://localhost:3002',
  'http://localhost:5173'
];

app.use(cors({
  origin: allowedOrigins,
  credentials: true
}));
app.use(express.json());

// Neo4j connection
const driver = neo4j.driver(
  process.env.NEO4J_URI,
  neo4j.auth.basic(process.env.NEO4J_USERNAME, process.env.NEO4J_PASSWORD)
);

// In-memory storage (use real database in production)
const users = new Map();
const subscriptions = new Map();

// Rate limiting
const freeTierLimit = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // 100 requests per window
  message: { error: 'Free tier limit: 10 searches per 15 minutes' }
});

const paidTierLimit = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 1000,
  message: { error: 'Rate limit exceeded' }
});

// Auth middleware
const authenticateToken = (req, res, next) => {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) {
    return res.status(401).json({ error: 'Access token required' });
  }

  jwt.verify(token, process.env.JWT_SECRET, (err, user) => {
    if (err) return res.status(403).json({ error: 'Invalid token' });
    req.user = user;
    next();
  });
};

// Check subscription
const checkSubscription = (req, res, next) => {
  const userId = req.user.userId;
  const userSub = subscriptions.get(userId);
  
  if (!userSub || userSub.status !== 'active') {
    req.subscriptionTier = 'free';
  } else {
    req.subscriptionTier = userSub.tier;
  }
  next();
};

// Apply rate limiting
const applyRateLimit = (req, res, next) => {
  if (req.subscriptionTier === 'free') {
    freeTierLimit(req, res, next);
  } else {
    paidTierLimit(req, res, next);
  }
};

// User registration
app.post('/auth/register', async (req, res) => {
  try {
    const { email, password } = req.body;
    
    if (users.has(email)) {
      return res.status(400).json({ error: 'User already exists' });
    }
    
    const hashedPassword = await bcrypt.hash(password, 10);
    const userId = Date.now().toString();
    
    users.set(email, {
      userId,
      email,
      password: hashedPassword,
      createdAt: new Date()
    });
    
    subscriptions.set(userId, {
      tier: 'free',
      status: 'active',
      searchesUsed: 0,
      resetDate: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000)
    });
    
    const token = jwt.sign({ userId, email }, process.env.JWT_SECRET, { expiresIn: '7d' });
    res.json({ token, message: 'Registration successful' });
  } catch (error) {
    console.error('Registration error:', error);
    res.status(500).json({ error: 'Registration failed' });
  }
});

// User login
app.post('/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    const user = users.get(email);
    
    if (!user || !await bcrypt.compare(password, user.password)) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }
    
    const token = jwt.sign(
      { userId: user.userId, email: user.email }, 
      process.env.JWT_SECRET,
      { expiresIn: '7d' }
    );
    res.json({ token });
  } catch (error) {
    console.error('Login error:', error);
    res.status(500).json({ error: 'Login failed' });
  }
});

// Enhanced singer search
app.post('/search/singers', authenticateToken, checkSubscription, applyRateLimit, async (req, res) => {
  const session = driver.session();
  
  // Helper function to safely convert Neo4j integers
  const safeToNumber = (value) => {
    if (value === null || value === undefined) return null;
    if (typeof value === 'object' && value.toNumber) return value.toNumber();
    return value;
  };
  
  try {
    const { query } = req.body;
    
    if (!query || query.length < 2) {
      return res.status(400).json({ error: 'Search query must be at least 2 characters' });
    }

    const hasAPOC = await checkAPOCAvailability();
    let singers = [];

    if (hasAPOC) {
      // Use APOC for diacritic-insensitive search
      const searchQuery = `
        MATCH (singer:Person)
        WHERE apoc.text.clean(toLower(singer.full_name)) CONTAINS apoc.text.clean(toLower($query))
           OR apoc.text.clean(toLower(singer.first_name)) CONTAINS apoc.text.clean(toLower($query))
           OR apoc.text.clean(toLower(singer.last_name)) CONTAINS apoc.text.clean(toLower($query))
        RETURN singer.full_name as name, singer
        ORDER BY 
          CASE 
            WHEN apoc.text.clean(toLower(singer.full_name)) = apoc.text.clean(toLower($query)) THEN 1
            WHEN apoc.text.clean(toLower(singer.full_name)) STARTS WITH apoc.text.clean(toLower($query)) THEN 2
            ELSE 3
          END,
          singer.full_name
        LIMIT 20
      `;
      
      const result = await session.run(searchQuery, { query });
      singers = result.records.map(record => ({
        name: record.get('name'),
        properties: {
          ...record.get('singer').properties,
          birth_year: safeToNumber(record.get('singer').properties.birth),
          death_year: safeToNumber(record.get('singer').properties.death),
          nationality: record.get('singer').properties.citizen
        }
      }));
    } else {
      // Fallback: Use JavaScript for diacritic removal
      const normalizedQuery = removeDiacritics(query.toLowerCase());
      
      const searchQuery = `
        MATCH (singer:Person)
        RETURN singer.full_name as name, singer
      `;
      
      const result = await session.run(searchQuery);
      
      // Filter and sort in JavaScript
      const allSingers = result.records.map(record => ({
        name: record.get('name'),
        properties: {
          ...record.get('singer').properties,
          birth_year: safeToNumber(record.get('singer').properties.birth),
          death_year: safeToNumber(record.get('singer').properties.death),
          nationality: record.get('singer').properties.citizen
        }
      }));

      singers = allSingers
        .filter(singer => {
          const normalizedName = removeDiacritics(singer.name.toLowerCase());
          const normalizedFirst = singer.properties.first_name ? 
            removeDiacritics(singer.properties.first_name.toLowerCase()) : '';
          const normalizedLast = singer.properties.last_name ? 
            removeDiacritics(singer.properties.last_name.toLowerCase()) : '';
          
          return normalizedName.includes(normalizedQuery) ||
                 normalizedFirst.includes(normalizedQuery) ||
                 normalizedLast.includes(normalizedQuery);
        })
        .sort((a, b) => {
          const normalizedA = removeDiacritics(a.name.toLowerCase());
          const normalizedB = removeDiacritics(b.name.toLowerCase());
          
          // Exact matches first
          if (normalizedA === normalizedQuery) return -1;
          if (normalizedB === normalizedQuery) return 1;
          
          // Starts with matches second
          if (normalizedA.startsWith(normalizedQuery) && !normalizedB.startsWith(normalizedQuery)) return -1;
          if (normalizedB.startsWith(normalizedQuery) && !normalizedA.startsWith(normalizedQuery)) return 1;
          
          // Alphabetical order
          return a.name.localeCompare(b.name);
        })
        .slice(0, 20);
    }
    
    // Track usage
    const userId = req.user.userId;
    const userSub = subscriptions.get(userId);
    if (userSub) {
      userSub.searchesUsed += 1;
      subscriptions.set(userId, userSub);
    }
    
    res.json({ singers });
  } catch (error) {
    console.error('Search error:', error);
    res.status(500).json({ error: 'Search failed' });
  } finally {
    await session.close();
  }
});

// Search operas
app.post('/search/operas', authenticateToken, checkSubscription, applyRateLimit, async (req, res) => {
  const session = driver.session();
  
  try {
    const { query } = req.body;
    
    if (!query || query.length < 2) {
      return res.status(400).json({ error: 'Search query must be at least 2 characters' });
    }

    // Always use JavaScript method for operas (more reliable)
    const normalizedQuery = removeDiacritics(query.toLowerCase());
    
    const searchQuery = `
      MATCH (opera:Opera)
      OPTIONAL MATCH (composer:Person)-[:WROTE]->(opera)
      RETURN opera.opera_name as opera_name,
             opera,
             collect(DISTINCT composer.full_name) as composers
    `;
    
    const result = await session.run(searchQuery);
    const allOperas = result.records.map(record => ({
      properties: {
        ...record.get('opera').properties,
        title: record.get('opera_name'),
        composer: record.get('composers').length > 0 ? record.get('composers')[0] : 'Unknown'
      }
    }));

    const operas = allOperas
      .filter(opera => {
        const normalizedTitle = opera.properties.title ? removeDiacritics(opera.properties.title.toLowerCase()) : '';
        const normalizedComposer = opera.properties.composer ? removeDiacritics(opera.properties.composer.toLowerCase()) : '';
        
        return normalizedTitle.includes(normalizedQuery) ||
               normalizedComposer.includes(normalizedQuery);
      })
      .sort((a, b) => a.properties.title.localeCompare(b.properties.title))
      .slice(0, 20);
    
    console.log(`Opera search for "${query}" returned ${operas.length} results`);
    
    // Track usage
    const userId = req.user.userId;
    const userSub = subscriptions.get(userId);
    if (userSub) {
      userSub.searchesUsed += 1;
      subscriptions.set(userId, userSub);
    }
    
    res.json({ operas });
  } catch (error) {
    console.error('Opera search error:', error);
    res.status(500).json({ error: 'Opera search failed' });
  } finally {
    await session.close();
  }
});

// Search books
app.post('/search/books', authenticateToken, checkSubscription, applyRateLimit, async (req, res) => {
  const session = driver.session();
  
  try {
    const { query } = req.body;
    
    if (!query || query.length < 2) {
      return res.status(400).json({ error: 'Search query must be at least 2 characters' });
    }

    // Always use JavaScript method for books (more reliable)
    const normalizedQuery = removeDiacritics(query.toLowerCase());
    
    const searchQuery = `
      MATCH (book:Book)
      OPTIONAL MATCH (author:Person)-[:AUTHORED]->(book)
      OPTIONAL MATCH (editor:Person)-[:EDITED]->(book)
      RETURN book.title as book_title,
             book,
             collect(DISTINCT author.full_name) as authors,
             collect(DISTINCT editor.full_name) as editors
    `;
    
    const result = await session.run(searchQuery);
    const allBooks = result.records.map(record => {
      const authors = record.get('authors') || [];
      const editors = record.get('editors') || [];
      
      // Filter out null/empty values
      const validAuthors = authors.filter(name => name && name.trim());
      const validEditors = editors.filter(name => name && name.trim());
      
      // Combine all authors and editors for searching
      const allContributors = [...validAuthors, ...validEditors];
      
      // Create display string with all contributors
      let authorEditor = 'Unknown';
      const contributorParts = [];
      
      if (validAuthors.length > 0) {
        contributorParts.push(...validAuthors);
      }
      
      if (validEditors.length > 0) {
        const editorsWithLabel = validEditors.map(editor => `${editor} (Editor)`);
        contributorParts.push(...editorsWithLabel);
      }
      
      if (contributorParts.length > 0) {
        authorEditor = contributorParts.join(', ');
      }
      
      return {
        properties: {
          title: record.get('book_title'),
          author: authorEditor,
          allContributors: allContributors // Keep all for searching
        }
      };
    });

    const books = allBooks
      .filter(book => {
        if (!book.properties.title) return false;
        
        const normalizedTitle = removeDiacritics(book.properties.title.toLowerCase());
        
        // Search in title
        if (normalizedTitle.includes(normalizedQuery)) return true;
        
        // Search in ALL contributors (authors and editors)
        return book.properties.allContributors.some(contributor => {
          const normalizedContributor = removeDiacritics(contributor.toLowerCase());
          return normalizedContributor.includes(normalizedQuery);
        });
      })
      .map(book => ({
        // Remove allContributors from final result
        properties: {
          title: book.properties.title,
          author: book.properties.author
        }
      }))
      .sort((a, b) => a.properties.title.localeCompare(b.properties.title))
      .slice(0, 20);
    
    console.log(`Book search for "${query}" returned ${books.length} results`);
    
    // Track usage
    const userId = req.user.userId;
    const userSub = subscriptions.get(userId);
    if (userSub) {
      userSub.searchesUsed += 1;
      subscriptions.set(userId, userSub);
    }
    
    res.json({ books });
  } catch (error) {
    console.error('Book search error:', error);
    res.status(500).json({ error: 'Book search failed' });
  } finally {
    await session.close();
  }
});

// Singer network details
app.post('/singer/network', authenticateToken, checkSubscription, applyRateLimit, async (req, res) => {
  const session = driver.session();
  
  // Helper function to safely convert Neo4j integers
  const safeToNumber = (value) => {
    if (value === null || value === undefined) return null;
    if (typeof value === 'object' && value.toNumber) return value.toNumber();
    return value;
  };

  const safeMapPerson = (person) => {
    if (!person || !person.properties) return {};
    return {
      ...person.properties,
      birth_year: safeToNumber(person.properties.birth),
      death_year: safeToNumber(person.properties.death),
      nationality: person.properties.citizen,
      // Remove the original birth/death to avoid conflicts
      birth: undefined,
      death: undefined
    };
  };

  // Helper specifically for relationship objects from queries
  const safeMapRelationshipPerson = (personObj) => {
    if (!personObj || !personObj.full_name) return null;
    return {
      full_name: personObj.full_name,
      voice_type: personObj.voice_type,
      birth_year: safeToNumber(personObj.birth_year),
      death_year: safeToNumber(personObj.death_year),
      relationship_source: personObj.relationship_source,
      relationship_type: personObj.relationship_type
    };
  };
  
  try {
    const { singerName, depth = 2 } = req.body;
    
    if (!singerName) {
      return res.status(400).json({ error: 'Singer name required' });
    }

    const networkQuery = `
      MATCH (center:Person {full_name: $singerName})
      
      // Get teaching connections (both directions)
      OPTIONAL MATCH teachingPath = (center)-[:TAUGHT|COACHED*1..2]-(connected:Person)
      WITH center, collect(DISTINCT connected) as connected_singers
      
      // Get family relationships - return as structured objects
      OPTIONAL MATCH (center)-[family:PARENT|SIBLING|SPOUSE|GRANDPARENT]-(relative:Person)
      
      // Get students (people this person taught) WITH relationship source
      OPTIONAL MATCH (center)-[taught:TAUGHT|COACHED]->(student:Person)
      
      // Get teachers (people who taught this person) WITH relationship source AND type  
      OPTIONAL MATCH (center)<-[learned:TAUGHT|COACHED]-(teacher:Person)
      
      // Get premiered operas WITH source
      OPTIONAL MATCH (center)-[premiere:PREMIERED_ROLE_IN]->(opera:Opera)
      
      // Get authored books
      OPTIONAL MATCH (center)-[:AUTHORED]->(book:Book)
      
      RETURN center,
             connected_singers,
             collect(DISTINCT {
               full_name: relative.full_name, 
               voice_type: relative.voice_type,
               birth_year: relative.birth,
               death_year: relative.death,
               relationship_type: type(family)
             }) as family,
             collect(DISTINCT {
               full_name: student.full_name, 
               voice_type: student.voice_type,
               birth_year: student.birth,
               death_year: student.death,
               relationship_source: taught.source,
               relationship_type: type(taught)
             }) as students,
             collect(DISTINCT {
               full_name: teacher.full_name, 
               voice_type: teacher.voice_type,
               birth_year: teacher.birth,
               death_year: teacher.death,
               relationship_source: learned.source,
               relationship_type: type(learned)
             }) as teachers,
             collect(DISTINCT {
               title: opera.opera_name, 
               role: premiere.role,
               source: premiere.source
             }) as premieredOperas,
             collect(DISTINCT {title: book.title}) as authoredBooks
    `;
    
    const result = await session.run(networkQuery, { singerName });

    if (result.records.length === 0) {
      return res.status(404).json({ error: 'Singer not found' });
    }

    const record = result.records[0];
    const center = record.get('center');
    const connectedSingers = record.get('connected_singers') || [];
    
    // Process all relationship data with proper number conversion
    const rawFamily = record.get('family') || [];
    const family = rawFamily.map(f => safeMapRelationshipPerson(f)).filter(f => f !== null);
    
    const rawStudents = record.get('students') || [];
    const students = rawStudents.map(s => safeMapRelationshipPerson(s)).filter(s => s !== null);
    
    const rawTeachers = record.get('teachers') || [];
    const teachers = rawTeachers.map(t => safeMapRelationshipPerson(t)).filter(t => t !== null);
    
    const premieredOperas = record.get('premieredOperas').filter(opera => opera.title) || [];
    const authoredBooks = record.get('authoredBooks').filter(book => book.title) || [];

    const works = {
      operas: premieredOperas,
      books: authoredBooks
    };

    console.log('Family found:', family.length, 'members');
    console.log('Students found:', students.length, 'students');
    console.log('Teachers found:', teachers.length, 'teachers');

    // Track usage
    const userId = req.user.userId;
    const userSub = subscriptions.get(userId);
    if (userSub) {
      userSub.searchesUsed += 1;
      subscriptions.set(userId, userSub);
    }

    res.json({
      center: center ? safeMapPerson(center) : null,
      connectedSingers: connectedSingers.map(s => s ? safeMapPerson(s) : {}),
      family: family,
      students: students,
      teachers: teachers,
      works: works,
      networkSize: connectedSingers.length + family.length
    });
    
  } catch (error) {
    console.error('Network query error:', error);
    res.status(500).json({ error: 'Failed to fetch singer network' });
  } finally {
    await session.close();
  }
});

// Opera details endpoint
app.post('/opera/details', authenticateToken, checkSubscription, applyRateLimit, async (req, res) => {
  const session = driver.session();
  
  try {
    const { operaName } = req.body;
    
    if (!operaName) {
      return res.status(400).json({ error: 'Opera name required' });
    }

    const operaQuery = `
      MATCH (opera:Opera {opera_name: $operaName})
      OPTIONAL MATCH (composer:Person)-[:WROTE]->(opera)
      OPTIONAL MATCH (singer:Person)-[premiere:PREMIERED_ROLE_IN]->(opera)
      RETURN opera,
             collect(DISTINCT composer.full_name)[0] as composer,
             collect(DISTINCT {singer: singer.full_name, role: premiere.role}) as premieredRoles
    `;
    
    const result = await session.run(operaQuery, { operaName });

    if (result.records.length === 0) {
      return res.status(404).json({ error: 'Opera not found' });
    }

    const record = result.records[0];
    const opera = record.get('opera');
    const composer = record.get('composer');
    const premieredRoles = record.get('premieredRoles').filter(role => role.singer);

    // Debug output
    console.log('Opera name searched:', operaName);
    console.log('Raw premiered roles from DB:', record.get('premieredRoles'));
    console.log('Filtered premiered roles:', premieredRoles);

    // Track usage
    const userId = req.user.userId;
    const userSub = subscriptions.get(userId);
    if (userSub) {
      userSub.searchesUsed += 1;
      subscriptions.set(userId, userSub);
    }

    res.json({
      opera: {
        ...opera.properties,
        composer: composer || 'Unknown'
      },
      premieredRoles: premieredRoles
    });
    
  } catch (error) {
    console.error('Opera details query error:', error);
    res.status(500).json({ error: 'Failed to fetch opera details' });
  } finally {
    await session.close();
  }
});

// Book details endpoint
app.post('/book/details', authenticateToken, checkSubscription, applyRateLimit, async (req, res) => {
  const session = driver.session();
  
  try {
    const { bookTitle } = req.body;
    
    if (!bookTitle) {
      return res.status(400).json({ error: 'Book title required' });
    }

    const bookQuery = `
      MATCH (book:Book {title: $bookTitle})
      OPTIONAL MATCH (author:Person)-[:AUTHORED]->(book)
      OPTIONAL MATCH (editor:Person)-[:EDITED]->(book)
      RETURN book,
             collect(DISTINCT author.full_name) as authors,
             collect(DISTINCT editor.full_name) as editors
    `;
    
    const result = await session.run(bookQuery, { bookTitle });

    if (result.records.length === 0) {
      return res.status(404).json({ error: 'Book not found' });
    }

    const record = result.records[0];
    const book = record.get('book');
    const authors = record.get('authors').filter(name => name);
    const editors = record.get('editors').filter(name => name);

    // Combine authors and editors, with authors taking priority
    let authorEditor = 'Unknown';
    if (authors.length > 0) {
      authorEditor = authors[0];
    } else if (editors.length > 0) {
      authorEditor = editors[0];
    }

    // Debug output
    console.log('Book title searched:', bookTitle);
    console.log('Authors found:', authors);
    console.log('Editors found:', editors);

    // Track usage
    const userId = req.user.userId;
    const userSub = subscriptions.get(userId);
    if (userSub) {
      userSub.searchesUsed += 1;
      subscriptions.set(userId, userSub);
    }

    res.json({
      book: {
        ...book.properties,
        author: authorEditor // This will be the clickable name in frontend
      }
    });
    
  } catch (error) {
    console.error('Book details query error:', error);
    res.status(500).json({ error: 'Failed to fetch book details' });
  } finally {
    await session.close();
  }
});

// Health check
app.get('/health', async (req, res) => {
  try {
    await driver.verifyConnectivity();
    res.json({ 
      status: 'healthy', 
      neo4j: 'connected',
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(503).json({ 
      status: 'unhealthy', 
      error: error.message 
    });
  }
});

// Get subscription status
app.get('/subscription/status', authenticateToken, (req, res) => {
  const userSub = subscriptions.get(req.user.userId);
  res.json({
    tier: userSub?.tier || 'free',
    status: userSub?.status || 'active',
    searchesUsed: userSub?.searchesUsed || 0,
    resetDate: userSub?.resetDate
  });
});

const distPath = path.join(__dirname, 'frontend', 'dist');
if (fs.existsSync(distPath)) {
  app.use(express.static(distPath));

  app.get('*', (req, res, next) => {
    const requestPath = req.path || '';
    const apiPrefixes = ['/auth', '/search', '/singer', '/opera', '/book', '/health', '/subscription'];

    if (apiPrefixes.some(prefix => requestPath.startsWith(prefix))) {
      return next();
    }

    res.sendFile(path.join(distPath, 'index.html'));
  });
}

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`🎵 Classical Music Genealogy API running on port ${PORT}`);
  console.log(`🎭 Enhanced features: lineages, analytics, improved search`);
});

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down gracefully...');
  await driver.close();
  process.exit(0);
});
