import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import Redis from "ioredis";
import { RedisQ } from "../src/index";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeRedis() {
  return new Redis({
    host: process.env.REDIS_HOSTNAME || "localhost",
    port: Number(process.env.REDIS_PORT) || 6379,
    username: process.env.REDIS_USERNAME || undefined,
    password: process.env.REDIS_PASSWORD || undefined,
    lazyConnect: true,
  });
}

/** Unique prefix per test so suites can run in parallel safely. */
function uniquePrefix() {
  return `test:${crypto.randomUUID()}:`;
}

/** Wait until predicate returns true, polling every 50 ms. */
async function waitUntil(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 5000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (true) {
    if (await predicate()) return;
    if (Date.now() > deadline) throw new Error("waitUntil timed out");
    await Bun.sleep(50);
  }
}

/** Collect all entries from a Redis Stream. */
async function readStream(redis: Redis, key: string) {
  return redis.xrange(key, "-", "+");
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

type TestEvents = {
  greet: { name: string };
  fail: { reason: string };
  noop: Record<string, never>;
};

let redis: Redis;
let q: RedisQ<TestEvents>;
let prefix: string;

beforeEach(async () => {
  redis = makeRedis();
  await redis.connect();
  prefix = uniquePrefix();
});

afterEach(async () => {
  // Stop queue if still running
  try {
    await q?.stop();
  } catch {}

  // Flush all keys created by this test
  const keys = await redis.keys(`${prefix}*`);
  if (keys.length > 0) await redis.del(...keys);

  await redis.quit();
});

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

describe("constructor", () => {
  test("accepts a bare Redis instance", () => {
    expect(() => {
      q = new RedisQ(redis);
    }).not.toThrow();
  });

  test("accepts a RedisQOptions object", () => {
    expect(() => {
      q = new RedisQ({ redis, streamKey: `${prefix}stream` });
    }).not.toThrow();
  });

  test("does not close the user's Redis instance after stop()", async () => {
    q = new RedisQ({ redis, streamKey: `${prefix}stream` });
    await q.start();
    await q.stop();
    // The user's connection should still work
    await expect(redis.ping()).resolves.toBe("PONG");
  });
});

// ---------------------------------------------------------------------------
// Basic processing
// ---------------------------------------------------------------------------

describe("add + on + start", () => {
  test("processes a job end-to-end", async () => {
    q = new RedisQ<TestEvents>({
      redis,
      streamKey: `${prefix}stream`,
      jobKey: `${prefix}job`,
      delayKey: `${prefix}delay`,
      dlqKey: `${prefix}dlq`,
    });

    const received: string[] = [];

    q.on("greet", (job) => {
      received.push(job.data.name);
    });

    await q.start();
    await q.add("greet", { name: "Alice" });
    await waitUntil(() => received.length === 1);

    expect(received).toEqual(["Alice"]);
  });

  test("processes multiple events in order", async () => {
    q = new RedisQ<TestEvents>({
      redis,
      streamKey: `${prefix}stream`,
      jobKey: `${prefix}job`,
      delayKey: `${prefix}delay`,
      dlqKey: `${prefix}dlq`,
      batchSize: 1,
    });

    const received: string[] = [];
    q.on("greet", (job) => {
      received.push(job.data.name);
    });

    await q.start();
    await q.add("greet", { name: "Alice" });
    await q.add("greet", { name: "Bob" });
    await q.add("greet", { name: "Carol" });

    await waitUntil(() => received.length === 3);
    expect(received).toEqual(["Alice", "Bob", "Carol"]);
  });

  test("job hash entry is cleaned up after success", async () => {
    q = new RedisQ<TestEvents>({
      redis,
      streamKey: `${prefix}stream`,
      jobKey: `${prefix}job`,
      delayKey: `${prefix}delay`,
      dlqKey: `${prefix}dlq`,
    });

    let processed = false;
    q.on("greet", () => {
      processed = true;
    });

    await q.start();
    const id = await q.add("greet", { name: "Alice" });

    await waitUntil(() => processed);
    const entry = await redis.hget(`${prefix}job`, id);
    expect(entry).toBeNull();
  });

  test("can add jobs before start()", async () => {
    q = new RedisQ<TestEvents>({
      redis,
      streamKey: `${prefix}stream`,
      jobKey: `${prefix}job`,
      delayKey: `${prefix}delay`,
      dlqKey: `${prefix}dlq`,
    });

    const received: string[] = [];
    q.on("greet", (job) => {
      received.push(job.data.name);
    });

    await q.add("greet", { name: "PreStart" });
    await q.start();

    await waitUntil(() => received.length === 1);
    expect(received[0]).toBe("PreStart");
  });
});

// ---------------------------------------------------------------------------
// Delayed jobs
// ---------------------------------------------------------------------------

describe("delayed jobs", () => {
  test("delayed job is not processed before its time", async () => {
    q = new RedisQ<TestEvents>({
      redis,
      streamKey: `${prefix}stream`,
      jobKey: `${prefix}job`,
      delayKey: `${prefix}delay`,
      dlqKey: `${prefix}dlq`,
      schedulerIntervalMs: 50,
    });

    const received: string[] = [];
    q.on("greet", (job) => {
      received.push(job.data.name);
    });

    await q.start();
    await q.add("greet", { name: "Delayed" }, { delay: 300 });

    await Bun.sleep(100);
    expect(received).toHaveLength(0);
  });

  test("delayed job is processed after its delay", async () => {
    q = new RedisQ<TestEvents>({
      redis,
      streamKey: `${prefix}stream`,
      jobKey: `${prefix}job`,
      delayKey: `${prefix}delay`,
      dlqKey: `${prefix}dlq`,
      schedulerIntervalMs: 50,
    });

    const received: string[] = [];
    q.on("greet", (job) => {
      received.push(job.data.name);
    });

    await q.start();
    await q.add("greet", { name: "Delayed" }, { delay: 200 });

    await waitUntil(() => received.length === 1, 3000);
    expect(received[0]).toBe("Delayed");
  });

  test("zero delay goes directly to stream, not sorted set", async () => {
    q = new RedisQ<TestEvents>({
      redis,
      streamKey: `${prefix}stream`,
      jobKey: `${prefix}job`,
      delayKey: `${prefix}delay`,
      dlqKey: `${prefix}dlq`,
    });

    const id = await q.add("greet", { name: "Instant" });

    const inDelay = await redis.zscore(`${prefix}delay`, id);
    expect(inDelay).toBeNull();

    // Poll — this.redis is a fresh duplicate that may commit slightly
    // after exec() resolves from the perspective of a second connection.
    await waitUntil(async () => {
      const entries = await readStream(redis, `${prefix}stream`);
      return entries.length >= 1;
    }, 2000);

    const entries = await readStream(redis, `${prefix}stream`);
    expect(entries.length).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Retries
// ---------------------------------------------------------------------------

describe("retries", () => {
  test("retries a failing job up to retryLimit times", async () => {
    q = new RedisQ<TestEvents>({
      redis,
      streamKey: `${prefix}stream`,
      jobKey: `${prefix}job`,
      delayKey: `${prefix}delay`,
      dlqKey: `${prefix}dlq`,
      retryLimit: 2,
      retryBaseDelayMs: 50,
      retryJitterMs: 0,
      schedulerIntervalMs: 30,
    });

    let attempts = 0;
    q.on("fail", () => {
      attempts++;
      throw new Error("boom");
    });

    await q.start();
    await q.add("fail", { reason: "test" });

    // 1 initial + 2 retries = 3 total handler calls, then DLQ
    await waitUntil(() => attempts >= 3, 5000);
    expect(attempts).toBe(3);
  });

  test("job ends up in DLQ after exhausting retries", async () => {
    q = new RedisQ<TestEvents>({
      redis,
      streamKey: `${prefix}stream`,
      jobKey: `${prefix}job`,
      delayKey: `${prefix}delay`,
      dlqKey: `${prefix}dlq`,
      retryLimit: 1,
      retryBaseDelayMs: 50,
      retryJitterMs: 0,
      schedulerIntervalMs: 30,
    });

    q.on("fail", () => {
      throw new Error("always fails");
    });

    await q.start();
    await q.add("fail", { reason: "test" });

    // Poll DLQ directly — handler increments attempts before the throw,
    // so waitUntil on attempts can race the catch block's DLQ write.
    await waitUntil(async () => {
      const entries = await readStream(redis, `${prefix}dlq`);
      return entries.length >= 1;
    }, 5000);

    const dlqEntries = await readStream(redis, `${prefix}dlq`);
    expect(dlqEntries.length).toBe(1);

    const raw = dlqEntries[0]?.[1];
    const jobField = raw?.indexOf("job");
    expect(jobField).not.toBe(-1);
    const payload = JSON.parse(raw![jobField! + 1]!);
    expect(payload.error).toBe("always fails");
    expect(payload.attempts).toBe(2);
  });

  test("job hash is removed after DLQ", async () => {
    q = new RedisQ<TestEvents>({
      redis,
      streamKey: `${prefix}stream`,
      jobKey: `${prefix}job`,
      delayKey: `${prefix}delay`,
      dlqKey: `${prefix}dlq`,
      retryLimit: 0,
      retryBaseDelayMs: 50,
      retryJitterMs: 0,
      schedulerIntervalMs: 30,
    });

    let done = false;
    q.on("fail", () => {
      done = true;
      throw new Error("x");
    });

    await q.start();
    const id = await q.add("fail", { reason: "test" });

    await waitUntil(() => done);
    await Bun.sleep(100);

    const entry = await redis.hget(`${prefix}job`, id);
    expect(entry).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Missing handler
// ---------------------------------------------------------------------------

describe("missing handler", () => {
  test("job with no handler goes to DLQ immediately", async () => {
    const errors: string[] = [];

    q = new RedisQ<TestEvents>({
      redis,
      streamKey: `${prefix}stream`,
      jobKey: `${prefix}job`,
      delayKey: `${prefix}delay`,
      dlqKey: `${prefix}dlq`,
      onError: (err) => {
        errors.push(err.message);
      },
    });

    await q.start();
    await q.add("greet", { name: "Ghost" });

    // Poll DLQ rather than relying on onError timing + fixed sleep.
    await waitUntil(async () => {
      const entries = await readStream(redis, `${prefix}dlq`);
      return entries.length >= 1;
    }, 3000);

    const dlqEntries = await readStream(redis, `${prefix}dlq`);
    expect(dlqEntries.length).toBe(1);
    expect(errors.some((e) => e.includes("No handler"))).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------

describe("cancel", () => {
  test("cancels a delayed job before it is dispatched", async () => {
    q = new RedisQ<TestEvents>({
      redis,
      streamKey: `${prefix}stream`,
      jobKey: `${prefix}job`,
      delayKey: `${prefix}delay`,
      dlqKey: `${prefix}dlq`,
      schedulerIntervalMs: 50,
    });

    const received: string[] = [];
    q.on("greet", (job) => {
      received.push(job.data.name);
    });

    await q.start();
    const id = await q.add("greet", { name: "CancelMe" }, { delay: 500 });
    const cancelled = await q.cancel(id);

    expect(cancelled).toBe(true);
    await Bun.sleep(700);
    expect(received).toHaveLength(0);
  });

  test("returns false for unknown job id", async () => {
    q = new RedisQ({ redis });
    const cancelled = await q.cancel("non-existent-id");
    expect(cancelled).toBe(false);
  });

  test("returns false for already-processed job", async () => {
    q = new RedisQ<TestEvents>({
      redis,
      streamKey: `${prefix}stream`,
      jobKey: `${prefix}job`,
      delayKey: `${prefix}delay`,
      dlqKey: `${prefix}dlq`,
    });

    let done = false;
    q.on("greet", () => {
      done = true;
    });

    await q.start();
    const id = await q.add("greet", { name: "Alice" });

    await waitUntil(() => done);
    const cancelled = await q.cancel(id);
    expect(cancelled).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

describe("getStats", () => {
  test("tracks added and processed", async () => {
    q = new RedisQ<TestEvents>({
      redis,
      streamKey: `${prefix}stream`,
      jobKey: `${prefix}job`,
      delayKey: `${prefix}delay`,
      dlqKey: `${prefix}dlq`,
    });

    q.on("greet", () => {});
    await q.start();

    await q.add("greet", { name: "A" });
    await q.add("greet", { name: "B" });

    await waitUntil(() => q.getStats().processed === 2);

    const stats = q.getStats();
    expect(stats.added).toBe(2);
    expect(stats.processed).toBe(2);
  });

  test("tracks retried and deadLettered", async () => {
    q = new RedisQ<TestEvents>({
      redis,
      streamKey: `${prefix}stream`,
      jobKey: `${prefix}job`,
      delayKey: `${prefix}delay`,
      dlqKey: `${prefix}dlq`,
      retryLimit: 1,
      retryBaseDelayMs: 50,
      retryJitterMs: 0,
      schedulerIntervalMs: 30,
    });

    let calls = 0;
    q.on("fail", () => {
      calls++;
      throw new Error("x");
    });

    await q.start();
    await q.add("fail", { reason: "test" });

    await waitUntil(() => calls >= 2, 5000);
    await Bun.sleep(200);

    const stats = q.getStats();
    expect(stats.retried).toBe(1);
    expect(stats.deadLettered).toBe(1);
    expect(stats.failed).toBe(1);
  });

  test("tracks cancelled", async () => {
    q = new RedisQ<TestEvents>({
      redis,
      streamKey: `${prefix}stream`,
      jobKey: `${prefix}job`,
      delayKey: `${prefix}delay`,
      dlqKey: `${prefix}dlq`,
    });

    const id = await q.add("greet", { name: "X" }, { delay: 10000 });
    await q.cancel(id);

    expect(q.getStats().cancelled).toBe(1);
  });

  test("getStats returns a snapshot, not a live reference", async () => {
    q = new RedisQ({ redis });
    const snap1 = q.getStats();
    await q.add("greet", { name: "X" });
    const snap2 = q.getStats();

    expect(snap1.added).toBe(0);
    expect(snap2.added).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Lifecycle hooks
// ---------------------------------------------------------------------------

describe("onProcessStart / onProcessEnd", () => {
  test("onProcessStart fires before handler, onProcessEnd after", async () => {
    const timeline: string[] = [];

    q = new RedisQ<TestEvents>({
      redis,
      streamKey: `${prefix}stream`,
      jobKey: `${prefix}job`,
      delayKey: `${prefix}delay`,
      dlqKey: `${prefix}dlq`,
      onProcessStart: () => {
        timeline.push("start");
      },
      onProcessEnd: () => {
        timeline.push("end");
      },
    });

    q.on("greet", () => {
      timeline.push("handler");
    });

    await q.start();
    await q.add("greet", { name: "Alice" });

    await waitUntil(() => timeline.length === 3);
    expect(timeline).toEqual(["start", "handler", "end"]);
  });

  test("onProcessEnd does not fire when handler throws", async () => {
    const ends: number[] = [];

    q = new RedisQ<TestEvents>({
      redis,
      streamKey: `${prefix}stream`,
      jobKey: `${prefix}job`,
      delayKey: `${prefix}delay`,
      dlqKey: `${prefix}dlq`,
      retryLimit: 0,
      retryBaseDelayMs: 50,
      retryJitterMs: 0,
      schedulerIntervalMs: 30,
      onProcessEnd: () => {
        ends.push(1);
      },
    });

    let called = false;
    q.on("fail", () => {
      called = true;
      throw new Error("x");
    });

    await q.start();
    await q.add("fail", { reason: "test" });

    await waitUntil(() => called);
    await Bun.sleep(200);
    expect(ends).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// onMetric
// ---------------------------------------------------------------------------

describe("onMetric", () => {
  test("emits metrics for added and processed", async () => {
    const metrics: string[] = [];

    q = new RedisQ<TestEvents>({
      redis,
      streamKey: `${prefix}stream`,
      jobKey: `${prefix}job`,
      delayKey: `${prefix}delay`,
      dlqKey: `${prefix}dlq`,
      onMetric: (m) => {
        metrics.push(m.name);
      },
    });

    q.on("greet", () => {});
    await q.start();
    await q.add("greet", { name: "Alice" });

    await waitUntil(() => metrics.includes("processed"));
    expect(metrics).toContain("added");
    expect(metrics).toContain("processed");
  });
});

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

describe("lifecycle", () => {
  test("start() is idempotent", async () => {
    q = new RedisQ({ redis });
    await Promise.all([q.start(), q.start(), q.start()]);
    // No error thrown, still running
    expect(q.getStats()).toBeDefined();
  });

  test("stop() after stop() does not throw", async () => {
    q = new RedisQ({ redis });
    await q.start();
    await q.stop();
    await expect(q.stop()).resolves.toBeUndefined();
  });

  test("start() after stop() throws", async () => {
    q = new RedisQ({ redis });
    await q.start();
    await q.stop();
    await expect(q.start()).rejects.toThrow("Cannot restart");
  });

  test("stop() resolves promptly (within 2x blockMs)", async () => {
    const blockMs = 200;
    q = new RedisQ<TestEvents>({
      redis,
      streamKey: `${prefix}stream`,
      jobKey: `${prefix}job`,
      delayKey: `${prefix}delay`,
      dlqKey: `${prefix}dlq`,
      blockMs,
      schedulerIntervalMs: 50,
      reclaimIntervalMs: 50,
    });

    q.on("greet", () => {});
    await q.start();

    const t0 = Date.now();
    await q.stop();
    const elapsed = Date.now() - t0;

    expect(elapsed).toBeLessThan(blockMs * 2 + 200);
  });

  test("onError is called for handler errors", async () => {
    const errors: Error[] = [];

    q = new RedisQ<TestEvents>({
      redis,
      streamKey: `${prefix}stream`,
      jobKey: `${prefix}job`,
      delayKey: `${prefix}delay`,
      dlqKey: `${prefix}dlq`,
      retryLimit: 0,
      retryBaseDelayMs: 50,
      retryJitterMs: 0,
      schedulerIntervalMs: 30,
      onError: (err) => {
        errors.push(err);
      },
    });

    q.on("fail", () => {
      throw new Error("handler boom");
    });
    await q.start();
    await q.add("fail", { reason: "test" });

    await waitUntil(() => errors.length > 0, 3000);
    expect(errors[0]?.message).toContain("handler boom");
  });
});
