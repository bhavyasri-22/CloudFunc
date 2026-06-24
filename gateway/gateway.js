const express = require("express");
const axios = require("axios");
const amqp = require("amqplib");
const { v4: uuidv4 } = require("uuid");
const cors = require("cors");

const app = express();
const PORT = process.env.PORT || 5001;

const REGISTRY_URL = process.env.REGISTRY_URL || "http://localhost:8080";
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://localhost:5672";
const QUEUE_NAME = "executions";

app.use(cors());
app.use(express.json({ limit: "1mb" }));

// ─────────────────────────────────────────────
// Auth Middleware
// ─────────────────────────────────────────────
function verifyAuth(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader) {
    console.warn("Warning: Missing Authorization header");
  } else {
    console.log("Authorization header verified:", authHeader);
  }
  next();
}

let channel;

// ─────────────────────────────────────────────
// Connect to RabbitMQ (with auto-retry)
// ─────────────────────────────────────────────
async function connectRabbitMQ() {
  try {
    const connection = await amqp.connect(RABBITMQ_URL);
    channel = await connection.createChannel();
    await channel.assertQueue(QUEUE_NAME, { durable: true });
    console.log("Gateway connected to RabbitMQ");
  } catch (err) {
    console.error("RabbitMQ Connection Failed, retrying in 5s...", err.message);
    setTimeout(connectRabbitMQ, 5000);
  }
}

connectRabbitMQ();

/**
 * POST /invoke
 * Body: { functionName, input }
 *
 * 1. Looks up function metadata + handler_code from Registry
 * 2. Creates a job record (status: queued)
 * 3. Publishes job (including handler_code) to RabbitMQ
 * 4. Returns 202 + jobId immediately
 *
 * The handler_code travels with the job so the Worker → Container Manager
 * can inject it into the running container without an extra Registry call.
 */
app.post("/invoke", verifyAuth, async (req, res) => {
  const { functionName, input } = req.body;

  if (!functionName) {
    return res.status(400).json({ error: "functionName is required" });
  }

  try {
    // 1. Verify function exists in registry and grab handler_code
    const registryRes = await axios.get(`${REGISTRY_URL}/function/${functionName}`);
    const { image, handler_code } = registryRes.data;

    // 2. Generate unique jobId
    const jobId = uuidv4();

    // 3. Create job record in DB
    await axios.post(`${REGISTRY_URL}/jobs`, {
      id: jobId,
      functionName,
    });

    // 4. Publish job payload to RabbitMQ — handler_code rides along
    const jobPayload = {
      jobId,
      functionName,
      payload: input || {},
      handlerCode: handler_code,   // ← user's handler injected at runtime
    };

    if (!channel) {
      throw new Error("Message queue channel is not ready");
    }

    channel.sendToQueue(
      QUEUE_NAME,
      Buffer.from(JSON.stringify(jobPayload)),
      { persistent: true }
    );

    console.log(`Job ${jobId} queued for function "${functionName}"`);

    // 5. Return 202 Accepted
    res.status(202).json({ jobId, status: "queued" });

  } catch (err) {
    if (err.response?.status === 404) {
      return res.status(404).json({ error: `Function '${functionName}' not registered` });
    }
    console.error("Invocation enqueue failed:", err.message);
    res.status(500).json({ error: "Failed to submit execution job" });
  }
});

/**
 * GET /jobs/:id
 * Fetches job status from Registry
 */
app.get("/jobs/:id", async (req, res) => {
  const { id } = req.params;

  try {
    const registryRes = await axios.get(`${REGISTRY_URL}/jobs/${id}`);
    res.json(registryRes.data);
  } catch (err) {
    if (err.response?.status === 404) {
      return res.status(404).json({ error: "Job not found" });
    }
    console.error("Failed to query job status:", err.message);
    res.status(500).json({ error: "Failed to query job status" });
  }
});

app.listen(PORT, () => {
  console.log(`Gateway Service running on port ${PORT}`);
});
