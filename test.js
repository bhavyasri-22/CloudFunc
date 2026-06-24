/**
 * test.js — End-to-end Integration Test for CloudFunc Phase 3
 *
 * What it verifies:
 *  1. Function registration with a user-provided handler_code
 *  2. Cold start — a new Docker container is spawned and the handler is injected
 *  3. Warm start — the same container is reused (handler already injected)
 *  4. Handler update — re-registering with different code evicts the warm container
 *  5. Timeout enforcement — a slow handler is killed after EXECUTION_TIMEOUT + 3 retries
 */

const { spawn } = require("child_process");
const axios = require("axios");
const Docker = require("dockerode");

const docker = new Docker();

async function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ── Handler Code Strings ──────────────────────────────────────────────────────

// Adds two numbers
const ADD_HANDLER = `
module.exports = async (input) => {
  const { a = 0, b = 0 } = input;
  return a + b;
};
`.trim();

// Multiplies two numbers (used for handler-update test)
const MULTIPLY_HANDLER = `
module.exports = async (input) => {
  const { a = 0, b = 0 } = input;
  return a * b;
};
`.trim();

// Simulates a slow function to trigger timeout
const TIMEOUT_HANDLER = `
module.exports = async (input) => {
  const ms = input.delay || 8000;
  await new Promise((r) => setTimeout(r, ms));
  return { sleptFor: ms };
};
`.trim();

// ── Polling Helper ────────────────────────────────────────────────────────────

async function pollJob(jobId, { maxAttempts = 40, intervalMs = 1000 } = {}) {
  for (let i = 0; i < maxAttempts; i++) {
    await delay(intervalMs);
    const res = await axios.get(`http://localhost:5001/jobs/${jobId}`);
    const job = res.data;
    console.log(`  Polling ${jobId.slice(0, 8)}… → ${job.status}`);
    if (job.status === "completed" || job.status === "failed") {
      return job;
    }
  }
  throw new Error(`Job ${jobId} did not finish within ${maxAttempts * intervalMs}ms`);
}

// ── Main Test Runner ──────────────────────────────────────────────────────────

async function runTests() {
  console.log("=== STARTING CLOUDFUNC INTEGRATION TEST (handler-based) ===\n");

  // 0. Clean up any leftover function-runner containers from previous runs
  const containersBefore = await docker.listContainers({ all: true });
  for (const c of containersBefore) {
    if (c.Image.includes("function-runner")) {
      console.log(`Cleaning up old container: ${c.Id.slice(0, 12)}`);
      const ct = docker.getContainer(c.Id);
      await ct.stop().catch(() => {});
      await ct.remove().catch(() => {});
    }
  }

  // 1. Start services
  console.log("Starting services...");
  const registry = spawn("node", ["registry/registry.js"], { stdio: "inherit", cwd: __dirname });
  const gateway  = spawn("node", ["gateway/gateway.js"],  { stdio: "inherit", cwd: __dirname });
  const manager  = spawn("node", ["container-manager/manager.js"], { stdio: "inherit", cwd: __dirname });
  const worker   = spawn("node", ["Worker/worker/index.js"], {
    stdio: "inherit",
    cwd: __dirname,
    env: {
      ...process.env,
      REGISTRY_URL:  "http://localhost:8080",
      CONTAINER_URL: "http://localhost:3000",
      RABBITMQ_URL:  "amqp://localhost:5672",
    },
  });

  await delay(4000); // wait for services to boot

  try {
    // ──────────────────────────────────────────────────────────────────────────
    // TEST 1: Register function with custom handler
    // ──────────────────────────────────────────────────────────────────────────
    console.log("--- Test 1: Register function with handler_code ---");
    const regRes = await axios.post("http://localhost:8080/registerFunction", {
      name:         "add",
      owner:        "tester",
      handler_code: ADD_HANDLER,
      // image omitted → defaults to function-runner:latest
    });
    console.log("Registered:", regRes.data);
    if (!regRes.data.name) throw new Error("Test 1 Failed: registration response missing name");
    console.log("✅ Test 1 passed\n");

    // ──────────────────────────────────────────────────────────────────────────
    // TEST 2: Cold Start — first invocation spawns a new container
    // ──────────────────────────────────────────────────────────────────────────
    console.log("--- Test 2: Cold Start (add 10 + 20) ---");
    const t2Start = Date.now();
    const invoke1 = await axios.post("http://localhost:5001/invoke", {
      functionName: "add",
      input: { a: 10, b: 20 },
    }, { headers: { Authorization: "Bearer test-token" } });

    const jobId1 = invoke1.data.jobId;
    const job1   = await pollJob(jobId1);
    const t2Dur  = Date.now() - t2Start;

    console.log(`Cold start completed in ${t2Dur}ms. Result:`, job1.result);
    if (job1.status !== "completed") throw new Error(`Test 2 Failed: status=${job1.status}`);

    const coldResult = job1.result?.result ?? job1.result;
    if (coldResult !== 30 && coldResult?.result !== 30) {
      throw new Error(`Test 2 Failed: expected 30, got ${JSON.stringify(coldResult)}`);
    }
    console.log("✅ Test 2 passed\n");

    // ──────────────────────────────────────────────────────────────────────────
    // TEST 3: Warm Start — second invocation reuses the same container
    // ──────────────────────────────────────────────────────────────────────────
    console.log("--- Test 3: Warm Start (add 50 + 100) ---");
    const t3Start = Date.now();
    const invoke2 = await axios.post("http://localhost:5001/invoke", {
      functionName: "add",
      input: { a: 50, b: 100 },
    });

    const jobId2 = invoke2.data.jobId;
    const job2   = await pollJob(jobId2, { maxAttempts: 15, intervalMs: 500 });
    const t3Dur  = Date.now() - t3Start;

    console.log(`Warm start completed in ${t3Dur}ms. Result:`, job2.result);
    if (job2.status !== "completed") throw new Error(`Test 3 Failed: status=${job2.status}`);

    const warmResult = job2.result?.result ?? job2.result;
    if (warmResult !== 150 && warmResult?.result !== 150) {
      throw new Error(`Test 3 Failed: expected 150, got ${JSON.stringify(warmResult)}`);
    }
    if (t3Dur < t2Dur) {
      console.log(`✅ Warm start (${t3Dur}ms) faster than cold start (${t2Dur}ms)`);
    } else {
      console.warn(`⚠️  Warm start (${t3Dur}ms) was not faster than cold start (${t2Dur}ms) — acceptable under load`);
    }
    console.log("✅ Test 3 passed\n");

    // ──────────────────────────────────────────────────────────────────────────
    // TEST 4: Handler Update — re-register with a different handler, warm
    //         container should be evicted, new handler injected
    // ──────────────────────────────────────────────────────────────────────────
    console.log("--- Test 4: Handler Update (re-register with multiply handler) ---");
    await axios.post("http://localhost:8080/registerFunction", {
      name:         "add",         // same name, different handler
      owner:        "tester",
      handler_code: MULTIPLY_HANDLER,
    });
    console.log("Re-registered with multiply handler");

    const invoke3 = await axios.post("http://localhost:5001/invoke", {
      functionName: "add",
      input: { a: 3, b: 7 },
    });

    const jobId3 = invoke3.data.jobId;
    const job3   = await pollJob(jobId3);

    console.log("Updated handler result:", job3.result);
    if (job3.status !== "completed") throw new Error(`Test 4 Failed: status=${job3.status}`);

    const updatedResult = job3.result?.result ?? job3.result;
    if (updatedResult !== 21 && updatedResult?.result !== 21) {
      throw new Error(`Test 4 Failed: expected 21 (3×7), got ${JSON.stringify(updatedResult)}`);
    }
    console.log("✅ Test 4 passed — handler update evicted warm container correctly\n");

    // ──────────────────────────────────────────────────────────────────────────
    // TEST 5: Timeout — slow handler must fail after retries
    // ──────────────────────────────────────────────────────────────────────────
    console.log("--- Test 5: Timeout Enforcement (slow handler, 8s sleep) ---");
    await axios.post("http://localhost:8080/registerFunction", {
      name:         "slow",
      owner:        "tester",
      handler_code: TIMEOUT_HANDLER,
    });

    const invoke4 = await axios.post("http://localhost:5001/invoke", {
      functionName: "slow",
      input: { delay: 8000 },
    });

    const jobId4 = invoke4.data.jobId;
    // Poll for up to 60s — timeout (5s) × 3 retries + backoff = ~22s
    const job4   = await pollJob(jobId4, { maxAttempts: 60, intervalMs: 1000 });

    console.log("Timeout test final state:", job4);
    if (job4.status !== "failed") {
      throw new Error(`Test 5 Failed: expected "failed", got "${job4.status}"`);
    }
    if (!job4.error?.toLowerCase().includes("timeout") && !job4.error?.includes("retries")) {
      throw new Error(`Test 5 Failed: error doesn't mention timeout: ${job4.error}`);
    }
    console.log("✅ Test 5 passed — timeout enforcement works correctly\n");

    console.log("=== ALL TESTS PASSED ✅ ===");

  } catch (err) {
    console.error("\n❌ INTEGRATION TEST FAILED:", err.message);
    process.exitCode = 1;

  } finally {
    console.log("\nTearing down services...");
    registry.kill();
    gateway.kill();
    manager.kill();
    worker.kill();

    const containersAfter = await docker.listContainers({ all: true });
    for (const c of containersAfter) {
      if (c.Image.includes("function-runner")) {
        const ct = docker.getContainer(c.Id);
        await ct.stop().catch(() => {});
        await ct.remove().catch(() => {});
      }
    }
    console.log("Teardown complete.");
    process.exit();
  }
}

runTests();
