const amqp = require("amqplib");
const axios = require("axios");

require("dotenv").config();

const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://localhost:5672";
const REGISTRY_URL = process.env.REGISTRY_URL || "http://localhost:8080";
const CONTAINER_URL = process.env.CONTAINER_URL || "http://localhost:3000";

const QUEUE_NAME = "executions";
const WORKER_COUNT = 3;

const sleep = (ms) => new Promise((res) => setTimeout(res, ms));

async function processJob(job) {
  const { jobId, functionName, payload, handlerCode } = job;

  // 1. Update job → running
  await axios.patch(`${REGISTRY_URL}/jobs/${jobId}`, {
    status: "running",
  });

  // 2. Call Container Manager — pass handlerCode so it can inject handler.js
  const response = await axios.post(`${CONTAINER_URL}/execute`, {
    jobId,
    functionName,
    payload,
    handlerCode,           // ← forwarded from RabbitMQ job message
  });

  // 3. Update job → completed
  await axios.patch(`${REGISTRY_URL}/jobs/${jobId}`, {
    status: "completed",
    result: response.data,
  });
}

async function executeWithRetry(job) {
  const MAX_RETRIES = 3;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      await processJob(job);
      console.log(`Job ${job.jobId} completed`);
      return;
    } catch (err) {
      console.error(`Job ${job.jobId} failed (attempt ${attempt}):`, err.message);
      if (attempt === MAX_RETRIES) {
        const errMsg = err.response?.data?.error || err.message;
        await axios.patch(`${REGISTRY_URL}/jobs/${job.jobId}`, {
          status: "failed",
          error: `Execution failed after retries: ${errMsg}`,
        });
        console.log(`Job ${job.jobId} marked as failed`);
        return;
      }
      await sleep(Math.pow(2, attempt - 1) * 1000); // 1s,2s,4s
    }
  }
}

async function startWorker(workerId, channel) {
  await channel.consume(
    QUEUE_NAME,
    async (msg) => {
      if (!msg) return;
      const job = JSON.parse(msg.content.toString());
      console.log(`Worker ${workerId} processing job ${job.jobId}`);
      try {
        await executeWithRetry(job);
        channel.ack(msg);
      } catch (err) {
        console.error("Unexpected error:", err.message);
        channel.ack(msg);
      }
    },
    { noAck: false }
  );
}

async function start() {
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();

  await channel.assertQueue(QUEUE_NAME, { durable: true });
  channel.prefetch(1);

  console.log("Worker connected to RabbitMQ");

  for (let i = 1; i <= WORKER_COUNT; i++) {
    startWorker(i, channel);
  }
}

start().catch(console.error);
