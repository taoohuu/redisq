<h1 align="center">RedisQ</h1>

<p align="center">
  <strong>Bring the familiar Node.js event-driven programming model to distributed systems.</strong>
</p>

<p align="center">
  A lightweight, type-safe event queue built on Redis Streams.
</p>

<p align="center">
  <a href="https://www.npmjs.com/package/@taoohuu/redisq">
    <img src="https://img.shields.io/npm/v/@taoohuu/redisq" alt="npm">
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

If you've ever used Node.js's `EventEmitter`, RedisQ will feel instantly familiar, except your events are processed reliably across multiple workers and even multiple machines.

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
- ✅ Type-safe payloads
- ✅ Redis Streams
- ✅ Consumer Groups
- ✅ Multi-worker processing
- ✅ Delayed events
- ✅ Automatic retries
- ✅ Dead-letter queue
- ✅ Pending recovery (`XAUTOCLAIM`)
- ✅ Graceful shutdown
- ✅ Runtime metrics
- ✅ Job cancellation

---

## Installation

```bash
bun add @taoohuu/redisq
```

or

```bash
npm install @taoohuu/redisq
```

---

# Quick Start

```ts
import Redis from "ioredis";
import { RedisQ } from "@taoohuu/redisq";

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

await q.start();

await q.add(
  "send-email",
  {
    to: "john@example.com",
    subject: "Welcome!",
    body: "Thanks for joining!",
  },
  {
    delay: 5000,
  },
);

await q.stop();
```

---

# Programming Model

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

# Architecture

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

 Worker A         Worker B

     │                │
     └────── ACK ─────┘

Failure
   │
   ▼

 Retry Queue

   │
   ▼

 Dead Letter Queue
```

---

# Public API

## `RedisQ<Events>`

```ts
new RedisQ(options)

add(event, data, options?)

cancel(jobId)

on(event, handler)

start()

stop()

getStats()
```

---

## add()

Publish a typed event.

```ts
await q.add(
  "send-email",
  {
    to: "john@example.com",
    subject: "Hi",
    body: "Hello",
  },
  {
    delay: 10000,
  },
);
```

Returns the generated Job ID.

---

## on()

Register an event handler.

```ts
q.on("resize-image", async (job) => {
  console.log(job.data.path);
});
```

The payload is automatically typed from your event interface.

---

## cancel()

Best-effort cancellation.

```ts
const cancelled = await q.cancel(jobId);
```

Returns

- `true` if removed
- `false` if no cancellable job exists

---

## start()

Starts workers and scheduler.

Creates the Consumer Group automatically if it does not exist.

---

## stop()

Gracefully stops workers and closes Redis connections.

A stopped instance cannot be restarted.

---

## getStats()

Returns runtime metrics for the current process.

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

# Type Safety

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
  job.data.width; // number
});
```

---

# Reliability

RedisQ provides **at-least-once delivery**.

Successful handlers are acknowledged and removed from the stream.

Failed handlers are retried using exponential backoff before eventually being moved to a Dead Letter Queue.

## Retries

- Exponential backoff
- Configurable retry limit
- Optional jitter

## Dead Letter Queue

Jobs exceeding the retry limit are moved into a dedicated DLQ stream.

## Pending Recovery

Workers automatically reclaim abandoned pending messages using `XAUTOCLAIM`.

This allows another worker to continue processing after crashes or unexpected shutdowns.

---

# Configuration

```ts
type RedisQOptions = {
  redis: Redis;

  streamKey?: string;
  delayKey?: string;
  jobKey?: string;
  dlqKey?: string;

  consumerGroup?: string;
  consumerName?: string;

  blockMs?: number;
  schedulerIntervalMs?: number;
  batchSize?: number;

  streamMaxLen?: number;
  dlqMaxLen?: number;

  retryLimit?: number;
  retryBaseDelayMs?: number;
  retryMaxDelayMs?: number;
  retryJitterMs?: number;

  reclaimIntervalMs?: number;
  reclaimMinIdleMs?: number;
  reclaimBatchSize?: number;

  onError?(error: Error, context: string): void;
  onProcessStart?(event: string, job: Job<QueueEvents, keyof QueueEvents>) => void;
  onProcessEnd?(event: string, job: Job<QueueEvents, keyof QueueEvents>) => void;
  onMetric?(metric: RedisQMetric): void;
};
```

---

# Delivery Semantics

RedisQ guarantees:

- At-least-once delivery
- Ordered events within a stream
- Reliable acknowledgements
- Automatic recovery
- Distributed processing

Handlers should be idempotent because retries and recovery may execute a job more than once.

---

# Production Tips

- Use one Consumer Group per application.
- Give every worker a unique Consumer Name.
- Monitor `onMetric`, `onProcessStart`, `onProcessEnd`, and `onError`.
- Configure stream trimming according to retention requirements.
- Enable Redis persistence for durability.

# License

MIT
