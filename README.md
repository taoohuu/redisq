<h1 align="center">RedisQ</h1>

<p align="center">
  <strong>Bring the familiar Node.js event-driven programming model to distributed systems.</strong>
</p>

<p align="center">
  A lightweight, type-safe event queue built on Redis Streams.
</p>

<p align="center">
  <a href="https://www.npmjs.com/package/@wnlx/redisq">
    <img src="https://img.shields.io/npm/v/@wnlx/redisq" alt="npm">
  </a>

  <a href="https://github.com/taoohuu/redisq">
    <img src="https://img.shields.io/github/stars/taoohuu/redisq?style=flat" alt="Stars">
  </a>

  <a href="https://github.com/taoohuu/redisq/blob/main/LICENSE">
    <img src="https://img.shields.io/github/license/taoohuu/redisq" alt="License">
  </a>

  <img src="https://img.shields.io/badge/Redis-Streams-red" alt="Redis Streams">

  <img src="https://img.shields.io/badge/TypeScript-5.x-blue" alt="TypeScript">
</p>

---

RedisQ is a lightweight, type-safe event queue for TypeScript built on **Redis Streams** and **Consumer Groups**.

If you've ever used Node.js's `EventEmitter`, RedisQ will feel instantly familiar — except your events are processed reliably across multiple workers and even multiple machines.

```ts
q.on("send-email", async (job) => {
  await sendEmail(job.data);
});

await q.add("send-email", {
  to: "john@example.com",
  subject: "Welcome!",
  body: "Thanks for joining!",
});
```

---

## Why RedisQ?

Most Redis queue libraries are job-oriented.

RedisQ is **event-oriented**.

Instead of defining processors and queues, you publish **typed events** and subscribe to them using a familiar API inspired by Node.js.

### Features

- ✅ Event-driven API
- ✅ Type-safe payloads with full inference
- ✅ Redis Streams + Consumer Groups
- ✅ Multi-worker processing
- ✅ Delayed events
- ✅ Automatic retries with exponential backoff
- ✅ Dead-letter queue (with retry method)
- ✅ Pending recovery (`XAUTOCLAIM`)
- ✅ Graceful shutdown
- ✅ Runtime metrics
- ✅ Job cancellation

---

## Installation

```bash
bun add @wnlx/redisq
```

or

```bash
npm install @wnlx/redisq
```

---

## Quick Start

RedisQ separates **producing** and **consuming**. A producer just constructs the queue and calls `add()`. A consumer additionally registers handlers and calls `startWorker()`.

### Producer

```ts
import Redis from "ioredis";
import { RedisQ } from "@wnlx/redisq";

interface Events {
  "send-email": {
    to: string;
    subject: string;
    body: string;
  };
}

const redis = new Redis("redis://localhost:6379");
const q = new RedisQ<Events>({ redis });

// No startWorker() needed — producers just add jobs
await q.add(
  "send-email",
  { to: "john@example.com", subject: "Welcome!", body: "Thanks for joining!" },
  { delay: 5000 },
);
```

### Consumer

```ts
import Redis from "ioredis";
import { RedisQ } from "@wnlx/redisq";

interface Events {
  "send-email": {
    to: string;
    subject: string;
    body: string;
  };
}

const redis = new Redis("redis://localhost:6379");

const q = new RedisQ<Events>({
  redis,
  onError(error, context) {
    console.error(context, error);
  },
});

q.on("send-email", async (job) => {
  console.log(job.data.to);
});

await q.startWorker();

// On shutdown:
await q.stopWorker();
```

---

## Programming Model

RedisQ follows the same mental model as Node.js events.

| Node.js          | RedisQ            |
| ---------------- | ----------------- |
| `emitter.on()`   | `q.on()`          |
| `emitter.emit()` | `q.add()`         |
| Same process     | Multiple workers  |
| In-memory        | Redis Streams     |
| Fire-and-forget  | Reliable delivery |

You write event-driven code exactly as you would in Node.js, while RedisQ handles distribution, retries, delayed execution, acknowledgements, and recovery.

---

## Architecture

```
       Producer

  q.add("send-email")
          │
          ▼
    Redis Stream
          │
   Consumer Group
          │
  ┌───────┴────────┐
  ▼                ▼

Worker A        Worker B

  │                │
  └────── ACK ─────┘

Failure
  │
  ▼

Retry Queue (delay sorted set)

  │
  ▼

Dead Letter Queue
```

---

## Public API

### `new RedisQ<Events>(options)`

Creates a new queue instance. RedisQ duplicates two internal Redis connections from the one you provide and manages their lifecycle. Your original connection is never closed by RedisQ unless specified in `startWorker(closeOriginalConnection?)`.

---

### `add(event, data, options?)`

Publish a typed event. Returns the generated job ID. Works on both producer and consumer instances, with or without `startWorker()`.

```ts
const jobId = await q.add(
  "send-email",
  { to: "john@example.com", subject: "Hi", body: "Hello" },
  { delay: 10000 }, // optional, milliseconds
);
```

Jobs with no `delay` (or `delay: 0`) are dispatched to the stream immediately. Jobs with a `delay` are held in a sorted set and promoted to the stream when their time comes.

---

### `on(event, handler)`

Register an event handler. The payload is automatically typed from your event interface. Only relevant on consumer instances.

```ts
q.on("resize-image", async (job) => {
  console.log(job.data.path); // string
  console.log(job.data.width); // number
});
```

---

### `cancel(jobId)`

Best-effort cancellation. Returns `true` if the job was removed, `false` otherwise.

```ts
const cancelled = await q.cancel(jobId);
```

> **Note:** Cancellation only works for jobs still in the delay queue. Once a job has been promoted to the stream it will be processed — `cancel()` returns `false` in that case.

---

### `retryDLQJobs(event?)`

Re-queues jobs from the Dead Letter Queue back into the active queue and returns the number of jobs requeued. Optionally filter by event name or an array of event names.

```ts
// Retry all DLQ jobs
const count = await q.retryDLQJobs();

// Retry a single event type
await q.retryDLQJobs("send-email");

// Retry multiple event types
await q.retryDLQJobs(["send-email", "resize-image"]);
```

---

### `startWorker(closeOriginalConnection?)`

Starts the worker, scheduler, and reclaim loops. Creates the Consumer Group automatically if it does not exist. Calling `startWorker()` multiple times is safe — concurrent calls wait for the same initialization to complete.

Only call this on **consumer** instances. Producer instances do not need it.

```ts
// Standard usage
await q.startWorker();

// If you no longer need the original Redis connection after starting:
await q.startWorker(true);
```

> Passing `true` closes the original connection you provided in options, since RedisQ has already duplicated it internally. Useful when the original connection was created solely for constructing RedisQ.

---

### `stopWorker()`

Gracefully shuts down all loops and closes both internal Redis connections. A stopped instance cannot be restarted.

---

### `getStats()`

Returns a snapshot of runtime metrics for the **current process**. In a multi-worker deployment each worker tracks its own counters independently.

```ts
type RedisQStats = {
  added: number;
  cancelled: number;
  processed: number;
  retried: number;
  failed: number;
  deadLettered: number;
  reclaimed: number;
};
```

---

## Type Safety

Payloads are fully inferred from your event interface — no casts needed anywhere.

```ts
interface Events {
  "send-email": {
    to: string;
    subject: string;
    body: string;
  };
  "resize-image": {
    path: string;
    width: number;
    height: number;
  };
}

const q = new RedisQ<Events>({ redis });

await q.add("resize-image", {
  path: "/tmp/image.png",
  width: 800,
  height: 600,
});

q.on("resize-image", async (job) => {
  job.data.width; // number ✓
});
```

The `onProcessStart` and `onProcessEnd` hooks are also fully typed — narrowing `event` automatically narrows `job.data`:

```ts
const q = new RedisQ<Events>({
  redis,
  onProcessStart(event, job) {
    if (event === "send-email") {
      console.log(job.data.to); // string ✓
    }
    if (event === "resize-image") {
      console.log(job.data.width); // number ✓
    }
  },
});
```

---

## Reliability

RedisQ provides **at-least-once delivery**.

Successful handlers are acknowledged and removed from the stream. Failed handlers are retried using exponential backoff before eventually being moved to a Dead Letter Queue.

Handlers should be **idempotent** — retries and crash recovery may execute a job more than once.

### Retries

Failed handlers are retried up to `retryLimit` times using exponential backoff with optional jitter. Between retries the job is held in the delay sorted set and re-promoted to the stream when its retry time comes.

### Dead Letter Queue

Jobs that exceed the retry limit are moved into a dedicated DLQ stream (`redisq:dlq` by default) with their final error message and attempt count attached. DLQ jobs can be requeued at any time by calling `retryDLQJobs()`.

### Pending Recovery

Workers periodically call `XAUTOCLAIM` to reclaim messages that have been pending longer than `reclaimMinIdleMs`. This allows another worker to pick up jobs abandoned by a crashed consumer.

---

## Configuration

```ts
interface RedisQOptions<Events> {
  // Required
  redis: Redis;

  // Redis key names
  streamKey?: string; // default: "redisq:stream"
  delayKey?: string; // default: "redisq:delay"
  jobKey?: string; // default: "redisq:job"
  dlqKey?: string; // default: "redisq:dlq"

  // Consumer identity
  consumerGroup?: string; // default: "redisq"
  consumerName?: string; // default: "consumer-<uuid>"

  // Tuning
  blockMs?: number; // default: 1000
  schedulerIntervalMs?: number; // default: 250
  batchSize?: number; // default: 50
  streamMaxLen?: number; // default: 10000
  dlqMaxLen?: number; // default: 10000

  // Retries
  retryLimit?: number; // default: 3
  retryBaseDelayMs?: number; // default: 1000
  retryMaxDelayMs?: number; // default: 60000
  retryJitterMs?: number; // default: 250

  // Reclaim
  reclaimIntervalMs?: number; // default: 2000
  reclaimMinIdleMs?: number; // default: 30000
  reclaimBatchSize?: number; // default: 50

  // Hooks
  onProcessStart?(
    event: EventName<Events>,
    job: Job<Events, EventName<Events>>,
  ): void;
  onProcessEnd?(
    event: EventName<Events>,
    job: Job<Events, EventName<Events>>,
  ): void;
  onError?(error: Error, context: string): void;
  onMetric?(metric: RedisQMetric): void;
}
```

---

## Delivery Semantics

RedisQ guarantees:

- At-least-once delivery
- Ordered processing within a stream
- Reliable acknowledgements via `XACK` + `XDEL`
- Automatic crash recovery via `XAUTOCLAIM`
- Distributed processing across multiple workers

---

## Production Tips

- Use one Consumer Group per application.
- Give every worker a unique `consumerName` (the default UUID is fine).
- Producer instances do not need to call `startWorker()` — construct and `add()` is sufficient.
- Monitor `onMetric`, `onProcessStart`, `onProcessEnd`, and `onError` for observability.
- Configure `streamMaxLen` and `dlqMaxLen` to match your retention requirements.
- Enable Redis persistence (`AOF` or `RDB`) for durability.
- Make handlers idempotent — at-least-once delivery means a job may run more than once.

---

## License

MIT
