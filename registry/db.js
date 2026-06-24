const { Pool } = require("pg");

const pool = new Pool({
  host: process.env.DB_HOST || "localhost",
  user: process.env.DB_USER || "postgres",
  password: process.env.DB_PASSWORD || "postgres",
  database: process.env.DB_NAME || "cloudfunc",
  port: parseInt(process.env.DB_PORT || "5433"),
});

// Initialize database tables on startup
async function initDb() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS functions (
        name         VARCHAR(255) PRIMARY KEY,
        owner        VARCHAR(255) NOT NULL,
        image        VARCHAR(255) NOT NULL DEFAULT 'function-runner:latest',
        handler_code TEXT        NOT NULL DEFAULT '',
        created_at   TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
      );
    `);

    // Migrate existing tables: add handler_code column if it was created
    // without it (safe to run repeatedly thanks to IF NOT EXISTS equivalent)
    await pool.query(`
      ALTER TABLE functions
        ADD COLUMN IF NOT EXISTS handler_code TEXT NOT NULL DEFAULT '';
    `);

    await pool.query(`
      ALTER TABLE functions
        ADD COLUMN IF NOT EXISTS image VARCHAR(255) NOT NULL DEFAULT 'function-runner:latest';
    `).catch(() => {}); // ignore if column already exists

    await pool.query(`
      CREATE TABLE IF NOT EXISTS jobs (
        id            VARCHAR(255) PRIMARY KEY,
        function_name VARCHAR(255) NOT NULL,
        status        VARCHAR(50)  NOT NULL,
        result        JSONB,
        error         TEXT,
        created_at    TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
        updated_at    TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
      );
    `);

    console.log("Database tables initialized successfully");
  } catch (err) {
    console.error("Database initialization failed:", err.message);
  }
}

initDb();

module.exports = pool;
