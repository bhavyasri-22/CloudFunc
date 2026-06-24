const express = require("express");
const pool = require("./db");
const cors = require("cors");

const app = express();
const PORT = process.env.PORT || 8080;

app.use(cors());
app.use(express.json({ limit: "1mb" }));

// ─────────────────────────────────────────────
// HELPER: Basic JS syntax validation
// Wraps the handler code in a function and uses
// the Function constructor to catch parse errors
// before we persist anything to the DB.
// ─────────────────────────────────────────────
function validateHandlerCode(code) {
  try {
    // We wrap it in a module-like context just to syntax-check it
    new Function("module", "exports", "require", code);
    return null; // no error
  } catch (err) {
    return err.message;
  }
}

/**
 * POST /registerFunction
 * Body: { name, owner, handler_code, image? }
 *
 * Registers (or updates) a function.
 * - handler_code is required — it must be valid JavaScript that
 *   assigns module.exports to an async function.
 * - image defaults to "function-runner:latest" if omitted.
 */
app.post("/registerFunction", async (req, res) => {
  const { name, owner, handler_code, image } = req.body;
  const resolvedImage = image || "function-runner:latest";

  if (!name || !owner || !handler_code) {
    return res.status(400).json({
      error: "name, owner, and handler_code are required"
    });
  }

  // Syntax-check the handler before storing
  const syntaxError = validateHandlerCode(handler_code);
  if (syntaxError) {
    return res.status(400).json({
      error: `handler_code syntax error: ${syntaxError}`
    });
  }

  try {
    const result = await pool.query(
      `INSERT INTO functions (name, owner, image, handler_code)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (name) DO UPDATE
         SET owner        = EXCLUDED.owner,
             image        = EXCLUDED.image,
             handler_code = EXCLUDED.handler_code,
             created_at   = NOW()
       RETURNING name, owner, image, created_at`,
      [name, owner, resolvedImage, handler_code]
    );

    res.status(201).json(result.rows[0]);
  } catch (err) {
    console.error("Failed to register function:", err.message);
    res.status(500).json({ error: "Failed to register function" });
  }
});

/**
 * GET /functions
 * Returns all registered functions (without handler_code for brevity)
 */
app.get("/functions", async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT name, owner, image, created_at FROM functions ORDER BY created_at DESC"
    );
    res.json(result.rows);
  } catch (err) {
    console.error("Failed to fetch functions:", err.message);
    res.status(500).json({ error: "Failed to fetch functions" });
  }
});

/**
 * GET /function/:name
 * Used by Gateway and Container Manager to lookup function metadata.
 * Returns handler_code so the caller can inject it into containers.
 */
app.get("/function/:name", async (req, res) => {
  const { name } = req.params;

  try {
    const result = await pool.query(
      `SELECT name, owner, image, handler_code, created_at
       FROM functions
       WHERE name = $1`,
      [name]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Function not found" });
    }

    res.json(result.rows[0]);
  } catch (err) {
    console.error("Database error during lookup:", err.message);
    res.status(500).json({ error: "Database error" });
  }
});

/**
 * POST /jobs
 * Creates a new job entry with status 'queued'
 */
app.post("/jobs", async (req, res) => {
  const { id, functionName } = req.body;

  if (!id || !functionName) {
    return res.status(400).json({ error: "id and functionName are required" });
  }

  try {
    const result = await pool.query(
      `INSERT INTO jobs (id, function_name, status)
       VALUES ($1, $2, 'queued')
       RETURNING *`,
      [id, functionName]
    );
    res.status(201).json(result.rows[0]);
  } catch (err) {
    console.error("Failed to create job:", err.message);
    res.status(500).json({ error: "Failed to create job" });
  }
});

/**
 * PATCH /jobs/:id
 * Updates job execution status, result, and/or error
 */
app.patch("/jobs/:id", async (req, res) => {
  const { id } = req.params;
  const { status, result, error } = req.body;

  if (!status) {
    return res.status(400).json({ error: "status is required" });
  }

  try {
    const query = `
      UPDATE jobs
      SET status = $1, result = $2, error = $3, updated_at = NOW()
      WHERE id = $4
      RETURNING *
    `;
    const dbResult = await pool.query(query, [
      status,
      result ? JSON.stringify(result) : null,
      error || null,
      id
    ]);

    if (dbResult.rows.length === 0) {
      return res.status(404).json({ error: "Job not found" });
    }

    res.json(dbResult.rows[0]);
  } catch (err) {
    console.error("Failed to update job status:", err.message);
    res.status(500).json({ error: "Failed to update job" });
  }
});

/**
 * GET /jobs/:id
 * Retrieves the status and result/error of a job
 */
app.get("/jobs/:id", async (req, res) => {
  const { id } = req.params;

  try {
    const result = await pool.query(
      `SELECT id, function_name, status, result, error, created_at, updated_at
       FROM jobs
       WHERE id = $1`,
      [id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Job not found" });
    }

    res.json(result.rows[0]);
  } catch (err) {
    console.error("Failed to retrieve job details:", err.message);
    res.status(500).json({ error: "Failed to fetch job" });
  }
});

app.listen(PORT, () => {
  console.log(`Function Registry Service running on port ${PORT}`);
});
