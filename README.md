<div align="center">
  <img src="./assets/Designer-9.png" height="120" alt="SnerdMQ Node.js Logo" />
  <h1>🚀 SnerdMQ Node.js SDK v0.3.4</h1>
  <p>The official Node.js & TypeScript SDK for SnerdMQ – A C-speed, zero-dependency background job engine.</p>

  [![npm version](https://img.shields.io/npm/v/snerdmq-node)](https://www.npmjs.com/package/snerdmq-node)
  [![License](https://img.shields.io/npm/l/snerdmq-node)](https://github.com/speed-nerd/snerdmq-node/blob/main/LICENSE)
  [![Docs](https://img.shields.io/badge/docs-speed--nerd.github.io-blue)](https://speed-nerd.github.io/docs/)
</div>

This is the official Node.js client for **SnerdMQ**. It acts as a lightweight, elegant wrapper over the underlying Rust background daemon. It handles all JSON-RPC communication, standard I/O piping, and event loop orchestration so you can write background jobs natively in JavaScript or TypeScript.

## ✨ v0.3.4 AI Features
- **Smart API Rate-Limiting**: Natively tracks `rateLimitGroup` execution velocity to prevent 429 "Too Many Requests" API errors.
- **Payload-Hashing Deduplication**: Automatically computes cryptographic hashes to drop duplicate tasks instantly.
- **Dynamic Float Prioritization**: A native Binary Max-Heap bypasses standard FIFO rules for high urgency tasks.
- **Progress Streaming & Live Dashboard**: Handlers can stream progress updates to a built-in React UI dashboard served by the SDK.
- **Zero Rust Required**: Our post-install script automatically downloads the pre-compiled C-speed Rust binary for your OS.
- **Native TypeScript**: Written in 100% TypeScript. Enjoy full autocomplete and strict type checking out of the box.
- **Zero Config**: No redis, no databases, no ports. Just start enqueuing jobs.

### ⚙️ Advanced Task Configuration (v0.3.4)
To power complex AI workflows, tasks can now be configured with advanced orchestration parameters:

* **`autoDedupe` (`boolean`)**: If set to `true`, the daemon computes a cryptographic hash of the `type` and `data`. If an identical payload is currently sitting in the queue pending execution, this new task is silently dropped. Excellent for preventing duplicate generative AI requests from trigger-happy users!
* **`urgencyScore` (`number`)**: A value (e.g. `0.99`) used to bypass the standard FIFO queue. SnerdMQ uses a true Binary Max-Heap to continually float tasks with the highest urgency score to the very front of the execution line. Standard tasks default to `0.0`.
* **`rateLimitGroup` (`string`)**: A custom string (e.g. `"openai_api"` or `"db_writes"`) that groups tasks together for backpressure control.
* **`maxPerMinute` (`number`)**: Used in conjunction with `rateLimitGroup`. If the queue processes more tasks in this group than the allowed limit within a 60-second rolling window, further tasks in this group are temporarily paused. This natively prevents 429 "Too Many Requests" errors when bursting third-party APIs.
* **`executeAt` (`string` | `Date`)**: A timestamp of when the job should be executed in the future.
* **`retryAfter` (`number`)**: Backoff in **hours** before a failed job is retried (default `0.0`). See *Cron Jobs vs. Retryable Jobs* below.
* **`cron` (`string`)**: A cron expression (e.g. `"0 * * * *"`) for recurring jobs. Shorthands like `"2h"` or `"10m"` are also supported.
* **`webhookUrl` (`string`)**: By providing a webhook URL, SnerdMQ will completely bypass your local Node handlers and dispatch the task payload via an HTTP POST request directly to the specified URL.
* **`maxExecutionSeconds` (`number`)**: Optional hard timeout in seconds. If execution takes longer, it's marked as failed.

### Note on Hard Timeouts (`maxExecutionSeconds`)
When `maxExecutionSeconds` is provided, the Node SDK wraps the execution of your handler using `Promise.race` against a `setTimeout`. If the task takes longer than the timeout, the SDK will mark it as failed. The background Rust daemon also enforces this timeout at the IPC level.

### 🌐 HTTP Webhooks (Serverless Execution)
You can configure a task to execute externally via an HTTP POST request. By setting a `webhookUrl`, the internal background processor will skip any registered handlers (`queue.registerHandler`) and directly invoke the HTTP endpoint.

If the HTTP endpoint returns a non-200 status code, it triggers a retry. If it permanently fails (reaches `maxRetries`), the Dead Letter Queue event is automatically fired via a final HTTP POST to the same `webhookUrl` but with the header `X-SnerdMQ-Event: MaxRetriesReached`.

### 🕒 Cron Jobs vs. Retryable Jobs
When using the new scheduling features, it is important to understand the difference between Cron and Retry behaviors:
> - **A Cron Job** is a *Repeatable Job* that executes again **only after a success**, on a fixed schedule.
> - **A Retryable Job** is a *Recovery Job* that executes again **only after a failure**, attempting to recover using the `retryAfter` backoff.
> - **Combined:** If a Cron Job fails, it temporarily uses `retryAfter` to retry until it recovers. Once it succeeds, it goes back to ticking on its standard cron schedule!

## 📦 Installation

```bash
npm install snerdmq-node
```

> **Note:** During installation, the SDK will automatically reach out to GitHub to download the correct SnerdMQ binary for your operating system (`macos`, `linux`, or `windows`). 

---

## ⚡ Quickstart

Using the SDK is incredibly simple. Initialize the queue, register your async handlers, and start enqueuing jobs!

```typescript
import { SnerdQueue } from 'snerdmq-node';

// 1. Initialize the daemon in the background
const queue = new SnerdQueue();

// 2. Register your background job logic
queue.registerHandler('send_email', async (data) => {
    console.log(`Sending email to ${data.to}...`);
    // ... your logic here (e.g., hitting SendGrid API)
});

// 3. Enqueue a job from anywhere in your codebase
queue.enqueue({
    id: `email-${Date.now()}`,
    type: 'send_email',
    data: { to: 'john@wick.com', subject: 'Continental Update' },
    maxRetries: 3,
    retryAfter: 0.5,          // Wait 30 minutes before retrying a failed job
    rateLimitGroup: 'email_api',
    maxPerMinute: 100,
});

// Need scheduling, deduplication, or serverless execution? All orchestration
// options are opt-in — combine only what you need:
queue.enqueue({
    id: `email-digest-${Date.now()}`,
    type: 'send_email',
    data: { to: 'john@wick.com', subject: 'Daily Digest' },
    cron: '0 8 * * *',                  // Run every day at 08:00
    autoDedupe: true,                   // Drop identical pending payloads
    urgencyScore: 0.99,                 // Float to the front of the queue
    webhookUrl: 'https://api.example.com/webhook', // Execute via HTTP instead of local handlers
    maxExecutionSeconds: 300,           // Hard timeout
});

// 4. Safely kill the daemon when your Node app exits (Required)
process.on('SIGINT', () => {
    console.log("Shutting down SnerdMQ...");
    queue.shutdown();
    process.exit(0);
});
```

### ☠️ Dead Letter Queue (Handling Permanent Failures)

When a task fails repeatedly and exhausts its `maxRetries`, the SnerdMQ daemon permanently moves it to the Dead Letter Queue. You can hook into this event to alert your team, update your database, or send a Slack message by registering a Max Retry Handler.

```typescript
// 5. Catch tasks that have permanently failed (Dead Letter Queue)
queue.registerMaxRetryHandler('send_email', async (data) => {
    console.error(`Email task failed after all retries! Data: ${JSON.stringify(data)}`);
});
```

---

## 📊 Live Dashboard

SnerdMQ ships with a built-in **React UI dashboard** served directly by the SDK — no extra services or ports to manage in your infrastructure. It gives you a real-time window into your queue:

- **Live stats**: total enqueued, processed, and failed jobs
- **Recent Jobs table**: per-task status (`queued`, `active`, `completed`, `failed`, `dead_letter`), retry counts, and badges showing which features a task uses (cron / webhook / timeout)
- **Real-time Progress Stream**: live output from `yieldProgress` calls in your handlers

```typescript
const queue = new SnerdQueue();

// Start the built-in dashboard on http://localhost:9090
queue.startDashboard(9090);

// ... register handlers, start listening, enqueue jobs ...
```

Then open **http://localhost:9090** in your browser. Updates are pushed to the page over WebSocket the moment jobs change state, and the dashboard also exposes a small JSON API (`/api/stats`, `/api/tasks`, `/api/progress`) if you want to build your own tooling on top.

> **Note:** `startDashboard` only serves the UI — your jobs keep running whether or not the dashboard is open.

---

## 📡 Progress Reporting

Long-running handlers can stream live updates to the Dashboard's Progress Stream (ideal for streaming LLM tokens or multi-step ETL work):

```typescript
queue.registerHandler('generate_report', async (data) => {
    for (let step = 1; step <= 10; step++) {
        await doWork(step);
        queue.yieldProgress(`Step ${step}/10 complete`);
    }
});
```

> `yieldProgress` must be called **inside a task handler** — the SDK tracks which task is currently executing so each update lands on the right job in the dashboard.

---

## 🧩 Queue Topology: One Queue or Many?

### ✅ Recommended: one queue, all job types (singleton)

Each `SnerdQueue` client spawns its own Rust daemon and **exclusively owns** its storage directory (`.snerdata` by default). The recommended pattern is **one client per application process**: register every job type on it and serve a single shared dashboard:

```typescript
import { SnerdQueue } from 'snerdmq-node';

// ONE queue client for the whole app
const queue = new SnerdQueue();

// Job type #1: image processing
queue.registerHandler('process_image', async (data) => {
    console.log(`Processing image: ${data.image_id}`);
});

// Job type #2: OTP emails — same queue, same daemon
queue.registerHandler('send_otp_email', async (data) => {
    console.log(`Sending OTP to: ${data.to}`);
});

// Both job types flow through the exact same queue
await queue.enqueue({ id: 'img-1', type: 'process_image', data: { image_id: 'abc123' }, maxRetries: 3, retryAfter: 0.5 });
await queue.enqueue({ id: 'otp-1', type: 'send_otp_email', data: { to: 'john@wick.com' }, maxRetries: 3, retryAfter: 0.5 });

// ONE dashboard shows every job type
queue.startDashboard(8080);
```

All job types share everything: the same persistent job log, retry/DLQ pipeline, rate-limit state, stats — and one dashboard at `http://localhost:8080` showing all of them.

### 🚫 Same storage twice = fails fast

The daemon takes an **exclusive OS-level lock** on its storage directory at startup. A second client on the same storage fails instead of silently double-executing your jobs:

```typescript
const first = new SnerdQueue(); // ✅ owns .snerdata
const second = new SnerdQueue(); // ❌ daemon refuses to start:
// "Another daemon is already running on storage '.snerdata'"
```

This applies across processes too — e.g. with Node's `cluster` module, every forked worker spawns its own daemon, so each worker needs its own `storagePath`.

### 🔀 Need multiple queues? Give each one its own storage

```typescript
const images = new SnerdQueue({ storagePath: '.snerdata-images' });
const emails = new SnerdQueue({ storagePath: '.snerdata-emails' });

images.startDashboard(8080); // separate dashboards, so separate ports
emails.startDashboard(8081);
```

Now you have two fully independent engines: separate job logs, separate rate-limit state, separate dashboards. Only split when you actually need isolation (different teams, different retention, independent monitoring) — otherwise the singleton is simpler and recommended.

---

## 🌍 Advanced: Distributed Scaling

Because the daemon exclusively locks its storage directory, scaling horizontally means **one queue per server**, each with its own storage. Your load balancer routes requests across servers, and every server processes the jobs it enqueued:

```typescript
import { SnerdQueue } from 'snerdmq-node';

// Each server runs its own daemon on its own storage dir (local disk works fine)
const queue = new SnerdQueue({
    storagePath: '/var/data/snerd' // per-server storage
});
```

A shared network drive (AWS EFS or NFS) is still a good home for that storage when a single instance needs durable state — e.g. a container that restarts but must keep its queue. Native OS file locking (`flock`) keeps writes safe — no Redis required.

*Built with ❤️ for John Wick tier engineering.*
