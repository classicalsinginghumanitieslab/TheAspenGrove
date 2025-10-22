# Classical Music Genealogy Backend

## Setup Instructions

### 1. Install Dependencies
```bash
npm install
```

### 2. Configure Environment Variables
Copy the `.env` file and update with your actual values:
- Set your Neo4j database credentials
- Change the JWT secret to a secure random string
- Update database URI if using a remote Neo4j instance
- Configure MySQL connection details or a full `MYSQL_URL`
- (Optional) Enable password reset emails by adding:
  - `SENDGRID_API_KEY` – SendGrid API key with Mail Send permissions
  - `SENDGRID_FROM_EMAIL` – Verified sender (e.g. `noreply@theaspengrove.org`)
  - `RESET_URL_BASE` – Base URL for the frontend (e.g. `https://theaspengrove.org`)
  - `PASSWORD_RESET_TOKEN_TTL_MINUTES` (optional, default `60`)
  - `PASSWORD_RESET_TOKEN_BYTES` (optional, default `32`)

### 3. Start Neo4j Database
Ensure your Neo4j database is running on `bolt://localhost:7687` (or update the URI in `.env`)

### 4. Run the Server
```bash
# Development mode (with nodemon)
npm run dev

# Production mode
npm start
```

The server will start on port 3001 (or the port specified in your `.env` file).

## API Endpoints

### Authentication
- `POST /auth/register` - Register new user
- `POST /auth/login` - Login user
- `POST /auth/forgot-password` - Generate a password reset token and email a reset link
- `POST /auth/reset-password` - Redeem a reset token and set a new password

### Search
- `POST /search/singers` - Search for singers
- `POST /search/operas` - Search for operas  
- `POST /search/books` - Search for books

### Network Data
- `POST /singer/network` - Get singer's network relationships
- `POST /opera/details` - Get opera details and relationships
- `POST /book/details` - Get book details

### Health
- `GET /health` - Health check endpoint

## Database Schema

Expected Neo4j node types:
- `:Singer` - Opera singers with properties like `full_name`, `voice_type`, etc.
- `:Opera` - Operas with properties like `title`, `composer`, etc.
- `:Book` - Books with properties like `title`, `author`, etc.
- `:User` - Application users for authentication

Expected relationships:
- `[:TAUGHT]` - Teacher-student relationships between singers
- `[:FAMILY]` - Family relationships between singers
- `[:PREMIERED_IN]` - Singer premiered role in opera
- `[:COMPOSED]` - Composer created opera
- `[:AUTHORED]` - Author wrote book
