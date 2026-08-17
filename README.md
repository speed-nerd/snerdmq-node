<div align="center">
  <img src="./assets/Designer-9.png" height="120" alt="SnerdMQ Node.js Logo" />
  <h1>🚀 SnerdMQ Node.js SDK v0.3.1</h1>
  <p>The official Node.js & TypeScript SDK for SnerdMQ – A C-speed, zero-dependency background job engine.</p>

  [![npm version](https://img.shields.io/npm/v/snerdmq-node)](https://www.npmjs.com/package/snerdmq-node)
  [![License](https://img.shields.io/npm/l/snerdmq-node)](https://github.com/greyhands2/snerdmq-node/blob/main/LICENSE)
</div>

This is the official Node.js client for **SnerdMQ**. It acts as a lightweight, elegant wrapper over the underlying Rust background daemon. It handles all JSON-RPC communication, standard I/O piping, and event loop orchestration so you can write background jobs natively in JavaScript or TypeScript.

## ✨ v0.3.1 AI Features
- **Smart API Rate-Limiting**: Natively tracks `rateLimitGroup` execution velocity to prevent 429 "Too Many Requests" API errors.
- **Payload-Hashing Deduplication**: Automatically computes cryptographic hashes to drop duplicate tasks instantly.
- **Dynamic Float Prioritization**: A native Binary Max-Heap bypasses standard FIFO rules for high urgency tasks.
- **Zero Rust Required**: Our post-install script automatically downloads the pre-compiled C-speed Rust binary for your OS.
- **Native TypeScript**: Written in 100% TypeScript. Enjoy full autocomplete and strict type checking out of the box.
- **Zero Config**: No redis, no databases, no ports. Just start enqueuing jobs.

### ⚙️ Advanced Task Configuration (v0.3.1)
To power complex AI workflows, tasks can now be configured with advanced orchestration parameters:

* **`autoDedupe` (`boolean`)**: If set to `true`, the daemon computes a cryptographic hash of the `type` and `data`. If an identical payload is currently sitting in the queue pending execution, this new task is silently dropped. Excellent for preventing duplicate generative AI requests from trigger-happy users!
* **`urgencyScore` (`number`)**: A value (e.g. `0.99`) used to bypass the standard FIFO queue. SnerdMQ uses a true Binary Max-Heap to continually float tasks with the highest urgency score to the very front of the execution line. Standard tasks default to `0.0`.
* **`rateLimitGroup` (`string`)**: A custom string (e.g. `"openai_api"` or `"db_writes"`) that groups tasks together for backpressure control.
* **`maxPerMinute` (`number`)**: Used in conjunction with `rateLimitGroup`. If the queue processes more tasks in this group than the allowed limit within a 60-second rolling window, further tasks in this group are temporarily paused. This natively prevents 429 "Too Many Requests" errors when bursting third-party APIs.
* **`executeAt` (`string` | `Date`)**: A timestamp of when the job should be executed in the future.
* **`cron` (`string`)**: A cron expression (e.g. `"0 * * * *"`) for recurring jobs. Shorthands like `"2h"` or `"10m"` are also supported.
* **`webhookUrl` (`string`)**: By providing a webhook URL, SnerdMQ will completely bypass your local Node handlers and dispatch the task payload via an HTTP POST request directly to the specified URL.

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

// 3. Enqueue a job from anywhere in your codebase (Now with v0.2.1 AI Features!)
queue.enqueue({
    id: `email-${Date.now()}`,
    type: 'send_email',
    data: { to: 'john@wick.com', subject: 'Continental Update' },
    maxRetries: 3,
    rateLimitGroup: 'email_api',


    autoDedupe: true,
    urgencyScore: 0.99,
    cron: "1h", // Runs every 1 hour!
    webhookUrl: "https://api.example.com/webhook" // Execute via HTTP instead of local handlers
});

// 4. (Optional) Safely kill the daemon when your Node app exits
process.on('SIGINT', () => {
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

## 🌍 Advanced: Distributed Scaling

By default, the SDK spins up the Rust daemon which writes the queue to a local file (`.snerdata/tasks/tasks.log`). 

If you have multiple Node.js servers running behind a load balancer and want them to share the exact same queue, simply mount a **Shared Network Drive** (like AWS EFS or NFS) to all of your servers and pass the shared path into the `SnerdQueue` options:

```typescript
import { SnerdQueue } from 'snerdmq-node';

// All 10 of your Node.js servers point to the exact same shared file!
// SnerdMQ's native OS file-locking guarantees zero data corruption.
const queue = new SnerdQueue({
    storagePath: '/mnt/aws-efs-shared-drive/snerd_tasks.log'
});
```

*Built with ❤️ for John Wick tier engineering.*
