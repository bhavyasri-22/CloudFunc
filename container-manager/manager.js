const express = require("express");
const axios = require("axios");
const Docker = require("dockerode");

const app = express();
const docker = new Docker();
app.use(express.json({ limit: "1mb" }));

const PORT = process.env.PORT || 3000;
const REGISTRY_URL = process.env.REGISTRY_URL || "http://localhost:8080";
const EXECUTION_TIMEOUT = parseInt(process.env.EXECUTION_TIMEOUT || "5000");

// ─────────────────────────────────────────────
// WARM CONTAINER POOL
// functionName → { containerId, hostPort, lastUsed, handlerHash }
// handlerHash lets us detect when a handler was re-registered so we
// evict the old container and inject the new code fresh.
// ─────────────────────────────────────────────
const containerPool = new Map();

// Simple hash so we can compare handler versions without storing the full code
function hashCode(str) {
  let h = 0;
  for (let i = 0; i < str.length; i++) {
    h = (Math.imul(31, h) + str.charCodeAt(i)) | 0;
  }
  return h;
}

// ─────────────────────────────────────────────
// INJECT HANDLER: write handler.js into running container via docker exec
// ─────────────────────────────────────────────
async function injectHandler(containerId, handlerCode) {
  const container = docker.getContainer(containerId);

  // Escape backslashes and backticks so the shell one-liner is safe
  const escaped = handlerCode
    .replace(/\\/g, "\\\\")
    .replace(/`/g, "\\`")
    .replace(/\$/g, "\\$");

  const exec = await container.exec({
    Cmd: ["node", "-e", `require('fs').writeFileSync('/app/handler.js', \`${escaped}\`)`],
    AttachStdout: true,
    AttachStderr: true,
  });

  await new Promise((resolve, reject) => {
    exec.start({ hijack: true, stdin: false }, (err, stream) => {
      if (err) return reject(err);
      // Drain stream to prevent backpressure; resolve on end
      stream.resume();
      stream.on("end", resolve);
      stream.on("error", reject);
    });
  });

  console.log(`Handler injected into container ${containerId.slice(0, 12)}`);
}

// ─────────────────────────────────────────────
// GET OR CREATE CONTAINER
// ─────────────────────────────────────────────
async function getOrCreateContainer(functionName, image, handlerCode) {
  const newHash = hashCode(handlerCode);
  let cached = containerPool.get(functionName);

  if (cached) {
    try {
      const container = docker.getContainer(cached.containerId);
      const state = await container.inspect();

      if (state.State.Running) {
        // If handler was re-registered (different hash) — evict and cold start
        if (cached.handlerHash !== newHash) {
          console.log(`Handler changed for "${functionName}" — evicting warm container and re-injecting`);
          containerPool.delete(functionName);
          await container.stop().catch(() => {});
          await container.remove().catch(() => {});
          // Fall through to cold start below
        } else {
          console.log(`Reusing warm container for function: ${functionName}`);
          cached.lastUsed = Date.now();
          return cached;
        }
      } else {
        console.log(`Warm container for "${functionName}" is stopped — removing from pool`);
        await container.remove({ force: true }).catch(() => {});
        containerPool.delete(functionName);
      }
    } catch {
      containerPool.delete(functionName);
    }
  }

  // ── Cold start ──
  console.log(`Cold start for function: "${functionName}" using image: ${image}`);

  const container = await docker.createContainer({
    Image: image,
    ExposedPorts: { "4000/tcp": {} },
    HostConfig: {
      PortBindings: {
        "4000/tcp": [{ HostPort: "0" }] // dynamic host port
      }
    }
  });

  await container.start();

  const info = await container.inspect();
  const hostPort = info.NetworkSettings.Ports["4000/tcp"][0].HostPort;
  const runnerURL = `http://localhost:${hostPort}`;

  // Poll /health until the runner is ready
  let healthy = false;
  for (let i = 0; i < 40; i++) {
    try {
      await axios.get(`${runnerURL}/health`, { timeout: 300 });
      healthy = true;
      break;
    } catch {
      await new Promise((r) => setTimeout(r, 200));
    }
  }

  if (!healthy) {
    await container.stop().catch(() => {});
    await container.remove().catch(() => {});
    throw new Error(`Runner container for "${functionName}" failed health check`);
  }

  // Inject the user's handler code into the freshly started container
  await injectHandler(container.id, handlerCode);

  const entry = {
    containerId: container.id,
    hostPort,
    lastUsed: Date.now(),
    handlerHash: newHash,
  };

  containerPool.set(functionName, entry);
  console.log(`Container ${container.id.slice(0, 12)} ready on port ${hostPort} for "${functionName}"`);
  return entry;
}

// ─────────────────────────────────────────────
// IDLE CONTAINER TTL CLEANUP (every 60s)
// ─────────────────────────────────────────────
setInterval(async () => {
  const now = Date.now();
  const idleTTL = 5 * 60 * 1000; // 5 minutes

  for (const [fnName, cached] of containerPool.entries()) {
    if (now - cached.lastUsed > idleTTL) {
      console.log(`Evicting idle container for "${fnName}" (idle > 5 min)`);
      containerPool.delete(fnName);
      try {
        const container = docker.getContainer(cached.containerId);
        await container.stop();
        await container.remove();
      } catch (err) {
        console.error(`Failed to remove idle container:`, err.message);
      }
    }
  }
}, 60_000);

// ─────────────────────────────────────────────
// POST /execute
// Body: { functionName, payload, handlerCode }
//
// Called by the Worker. Spins up (or reuses) a container,
// injects handler.js, then POSTs payload to /run.
// ─────────────────────────────────────────────
app.post("/execute", async (req, res) => {
  const { functionName, payload, handlerCode } = req.body;

  if (!functionName) {
    return res.status(400).json({ error: "functionName is required" });
  }
  if (!handlerCode) {
    return res.status(400).json({ error: "handlerCode is required — register the function with handler_code first" });
  }

  try {
    // Look up the image to use from registry (handler_code also returned but
    // we already have it from the job payload — avoids a second DB round-trip)
    const registryRes = await axios.get(`${REGISTRY_URL}/function/${functionName}`);
    const { image } = registryRes.data;

    const cached = await getOrCreateContainer(functionName, image, handlerCode);
    const runnerURL = `http://localhost:${cached.hostPort}/run`;

    console.log(`Executing "${functionName}" → ${runnerURL}`);

    const response = await axios.post(runnerURL, payload || {}, {
      timeout: EXECUTION_TIMEOUT,
    });

    cached.lastUsed = Date.now();
    res.json(response.data);

  } catch (err) {
    console.error(`Execution error for "${functionName}":`, err.message);
    res.status(500).json({
      success: false,
      result: null,
      error: err.message,
    });
  }
});

// ─────────────────────────────────────────────
// START SERVER
// ─────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`Container Manager running on port ${PORT}`);
});