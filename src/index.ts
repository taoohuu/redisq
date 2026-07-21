import type Redis from "ioredis";
import type {
  EventName,
  Job,
  RedisQMetric,
  QueueEvents,
  RedisQOptions,
  RedisQStats,
  QueueOptions,
} from "./types";

type StoredJob<Events extends QueueEvents> = {
  id: string;
  event: EventName<Events>;
  data: Events[EventName<Events>];
  createdAt: number;
  scheduledAt: number;
  attempts: number;
};

type RedisQInput = Redis | RedisQOptions;

const DEFAULT_STREAM_KEY = "redisq:stream";
const DEFAULT_DELAY_KEY = "redisq:delay";
const DEFAULT_JOB_KEY = "redisq:job";
const DEFAULT_DLQ_KEY = "redisq:dlq";
const DEFAULT_CONSUMER_GROUP = "redisq";
const DEFAULT_BLOCK_MS = 1000;
const DEFAULT_SCHEDULER_INTERVAL_MS = 250;
const DEFAULT_BATCH_SIZE = 50;
const DEFAULT_STREAM_MAX_LEN = 10000;
const DEFAULT_DLQ_MAX_LEN = 10000;
const DEFAULT_RETRY_LIMIT = 3;
const DEFAULT_RETRY_BASE_DELAY_MS = 1000;
const DEFAULT_RETRY_MAX_DELAY_MS = 60_000;
const DEFAULT_RETRY_JITTER_MS = 250;
const DEFAULT_RECLAIM_INTERVAL_MS = 2000;
const DEFAULT_RECLAIM_MIN_IDLE_MS = 30_000;
const DEFAULT_RECLAIM_BATCH_SIZE = 50;

export class RedisQ<Events extends QueueEvents = QueueEvents> {
  private readonly redis: Redis;
  private readonly workerRedis: Redis;
  private readonly streamKey: string;
  private readonly delayKey: string;
  private readonly jobKey: string;
  private readonly dlqKey: string;
  private readonly consumerGroup: string;
  private readonly consumerName: string;
  private readonly blockMs: number;
  private readonly schedulerIntervalMs: number;
  private readonly batchSize: number;
  private readonly streamMaxLen: number;
  private readonly dlqMaxLen: number;
  private readonly retryLimit: number;
  private readonly retryBaseDelayMs: number;
  private readonly retryMaxDelayMs: number;
  private readonly retryJitterMs: number;
  private readonly reclaimIntervalMs: number;
  private readonly reclaimMinIdleMs: number;
  private readonly reclaimBatchSize: number;
  private readonly onError?: (error: Error, context: string) => void;
  private readonly onProcess?: (
    event: string,
    job: Job<QueueEvents, keyof QueueEvents>,
  ) => void;
  private readonly onMetric?: (metric: RedisQMetric) => void;
  private readonly handlers = new Map<
    string,
    (job: Job<Events>) => Promise<void> | void
  >();
  private readonly stats: RedisQStats = {
    added: 0,
    cancelled: 0,
    processed: 0,
    retried: 0,
    failed: 0,
    deadLettered: 0,
    reclaimed: 0,
  };
  private running = false;
  private stopped = false;
  private startPromise: Promise<void> | null = null;
  private workerPromise: Promise<void> | null = null;
  private schedulerPromise: Promise<void> | null = null;
  private reclaimPromise: Promise<void> | null = null;

  constructor(input: RedisQInput);
  constructor(connection: Redis);
  constructor(input: RedisQInput | Redis) {
    const options = this.normalizeOptions(input);

    const rawPrefix = options.redis.options?.keyPrefix;
    const keyPrefix = typeof rawPrefix === "string" ? rawPrefix : "";

    // Use internal clients without ioredis keyPrefix and apply prefix ourselves.
    // This avoids command-specific keyPrefix inconsistencies (e.g. Streams commands).
    this.redis = options.redis.duplicate({ keyPrefix: "" });
    this.workerRedis = options.redis.duplicate({ keyPrefix: "" });
    this.streamKey = this.applyPrefix(
      options.streamKey ?? DEFAULT_STREAM_KEY,
      keyPrefix,
    );
    this.delayKey = this.applyPrefix(
      options.delayKey ?? DEFAULT_DELAY_KEY,
      keyPrefix,
    );
    this.jobKey = this.applyPrefix(
      options.jobKey ?? DEFAULT_JOB_KEY,
      keyPrefix,
    );
    this.dlqKey = this.applyPrefix(
      options.dlqKey ?? DEFAULT_DLQ_KEY,
      keyPrefix,
    );
    this.consumerGroup = options.consumerGroup ?? DEFAULT_CONSUMER_GROUP;
    this.consumerName =
      options.consumerName ?? `consumer-${crypto.randomUUID()}`;
    this.blockMs = Math.max(1, options.blockMs ?? DEFAULT_BLOCK_MS);
    this.schedulerIntervalMs = Math.max(
      1,
      options.schedulerIntervalMs ?? DEFAULT_SCHEDULER_INTERVAL_MS,
    );
    this.batchSize = Math.max(1, options.batchSize ?? DEFAULT_BATCH_SIZE);
    this.streamMaxLen = Math.max(
      1,
      options.streamMaxLen ?? DEFAULT_STREAM_MAX_LEN,
    );
    this.dlqMaxLen = Math.max(1, options.dlqMaxLen ?? DEFAULT_DLQ_MAX_LEN);
    this.retryLimit = Math.max(0, options.retryLimit ?? DEFAULT_RETRY_LIMIT);
    this.retryBaseDelayMs = Math.max(
      1,
      options.retryBaseDelayMs ?? DEFAULT_RETRY_BASE_DELAY_MS,
    );
    this.retryMaxDelayMs = Math.max(
      this.retryBaseDelayMs,
      options.retryMaxDelayMs ?? DEFAULT_RETRY_MAX_DELAY_MS,
    );
    this.retryJitterMs = Math.max(
      0,
      options.retryJitterMs ?? DEFAULT_RETRY_JITTER_MS,
    );
    this.reclaimIntervalMs = Math.max(
      1,
      options.reclaimIntervalMs ?? DEFAULT_RECLAIM_INTERVAL_MS,
    );
    this.reclaimMinIdleMs = Math.max(
      1,
      options.reclaimMinIdleMs ?? DEFAULT_RECLAIM_MIN_IDLE_MS,
    );
    this.reclaimBatchSize = Math.max(
      1,
      options.reclaimBatchSize ?? DEFAULT_RECLAIM_BATCH_SIZE,
    );
    this.onError = options.onError;
    this.onMetric = options.onMetric;
    this.onProcess = options.onProcess;
  }

  async add<Event extends EventName<Events>>(
    event: Event,
    data: Events[Event],
    options?: QueueOptions,
  ): Promise<string> {
    const now = Date.now();
    const delay = Math.max(0, options?.delay ?? 0);
    const job: StoredJob<Events> = {
      id: crypto.randomUUID(),
      event: event,
      data: data,
      createdAt: now,
      scheduledAt: now + delay,
      attempts: 0,
    };
    const payload = JSON.stringify(job);
    const transaction = this.redis.multi();
    transaction.hset(this.jobKey, job.id, payload);

    if (delay > 0) {
      transaction.zadd(this.delayKey, job.scheduledAt, job.id);
    } else {
      transaction.xadd(
        this.streamKey,
        "MAXLEN",
        "~",
        this.streamMaxLen,
        "*",
        "jobId",
        job.id,
      );
    }

    const result = await transaction.exec();

    if (result === null) {
      throw new Error("Failed to add job");
    }

    this.recordMetric("added", 1, "add");

    return job.id;
  }

  async cancel(jobId: string): Promise<boolean> {
    const transaction = this.redis.multi();
    transaction.hdel(this.jobKey, jobId);
    transaction.zrem(this.delayKey, jobId);

    const result = await transaction.exec();

    if (result === null) {
      throw new Error("Failed to cancel job");
    }

    const removedJob = Number(result[0]?.[1] ?? 0);
    const removedDelay = Number(result[1]?.[1] ?? 0);
    const wasCancelled = removedJob > 0 || removedDelay > 0;

    if (wasCancelled) {
      this.recordMetric("cancelled", 1, "cancel");
    }

    return wasCancelled;
  }

  getStats(): RedisQStats {
    return { ...this.stats };
  }

  on<Event extends EventName<Events>>(
    event: Event,
    handler: (job: Job<Events, Event>) => Promise<void> | void,
  ): void {
    this.handlers.set(
      event,
      handler as (job: Job<Events>) => Promise<void> | void,
    );
  }

  async start(): Promise<void> {
    if (this.stopped) {
      throw new Error("Cannot restart a stopped RedisQ instance");
    }

    if (this.running) {
      return this.startPromise ?? Promise.resolve();
    }

    this.running = true;
    this.startPromise = this.initialize()
      .catch((error) => {
        this.running = false;
        throw error;
      })
      .finally(() => {
        this.startPromise = null;
      });

    await this.startPromise;
  }

  async stop(): Promise<void> {
    this.running = false;
    this.stopped = true;

    await this.startPromise?.catch(() => undefined);
    await Promise.allSettled([
      this.workerPromise,
      this.schedulerPromise,
      this.reclaimPromise,
    ]);

    try {
      await this.workerRedis.quit();
    } catch {
      this.workerRedis.disconnect();
    }
  }

  private normalizeOptions(input: RedisQInput | Redis): RedisQOptions {
    if (this.isRedisQOptions(input)) {
      return input;
    }

    return { redis: input };
  }

  private isRedisQOptions(input: RedisQInput | Redis): input is RedisQOptions {
    return typeof input === "object" && input !== null && "redis" in input;
  }

  private applyPrefix(key: string, prefix: string): string {
    if (prefix.length === 0 || key.startsWith(prefix)) {
      return key;
    }

    return `${prefix}${key}`;
  }

  private async initialize(): Promise<void> {
    await this.ensureConsumerGroup();

    this.workerPromise = this.runWorkerLoop();
    this.schedulerPromise = this.runSchedulerLoop();
    this.reclaimPromise = this.runReclaimLoop();
  }

  private async ensureConsumerGroup(): Promise<void> {
    try {
      await this.redis.xgroup(
        "CREATE",
        this.streamKey,
        this.consumerGroup,
        "0",
        "MKSTREAM",
      );
    } catch (error) {
      if (!this.isBusyGroupError(error)) {
        throw error;
      }
    }
  }

  private isBusyGroupError(error: unknown): boolean {
    return error instanceof Error && error.message.includes("BUSYGROUP");
  }

  private isNoGroupError(error: unknown): boolean {
    return error instanceof Error && error.message.includes("NOGROUP");
  }

  private async recoverConsumerGroup(): Promise<void> {
    try {
      await this.ensureConsumerGroup();
    } catch (error) {
      this.reportError(error, "consumer_group_recovery");
    }
  }

  private async runWorkerLoop(): Promise<void> {
    while (this.running) {
      try {
        const result = await this.workerRedis.xreadgroup(
          "GROUP",
          this.consumerGroup,
          this.consumerName,
          "COUNT",
          this.batchSize,
          "BLOCK",
          this.blockMs,
          "STREAMS",
          this.streamKey,
          ">",
        );

        if (!this.running || result === null) {
          continue;
        }

        for (const [, entries] of result as Array<
          [string, Array<[string, string[]]>]
        >) {
          for (const [streamEntryId, fields] of entries) {
            if (!this.running) {
              return;
            }

            const job = await this.getJobFromStreamEntry(fields);
            if (job === null) {
              await this.ackAndDeleteStreamEntry(streamEntryId);
              continue;
            }

            await this.processJob(streamEntryId, job);
          }
        }
      } catch (error) {
        if (!this.running) {
          return;
        }

        if (this.isNoGroupError(error)) {
          await this.recoverConsumerGroup();
          await this.sleep(this.blockMs);
          continue;
        }

        await this.sleep(this.blockMs);
        this.reportError(error, "worker_loop");
      }
    }
  }

  private async runReclaimLoop(): Promise<void> {
    while (this.running) {
      try {
        const reclaimed = await this.reclaimPendingJobs();

        if (!this.running) {
          return;
        }

        if (reclaimed === 0) {
          await this.sleep(this.reclaimIntervalMs);
        }
      } catch (error) {
        if (!this.running) {
          return;
        }

        if (this.isNoGroupError(error)) {
          await this.recoverConsumerGroup();
          await this.sleep(this.reclaimIntervalMs);
          continue;
        }

        await this.sleep(this.reclaimIntervalMs);
        this.reportError(error, "reclaim_loop");
      }
    }
  }

  private async runSchedulerLoop(): Promise<void> {
    while (this.running) {
      try {
        const moved = await this.drainDelayedJobs();

        if (!this.running) {
          return;
        }

        if (!moved) {
          await this.sleep(this.schedulerIntervalMs);
        }
      } catch (error) {
        if (!this.running) {
          return;
        }

        await this.sleep(this.schedulerIntervalMs);
        this.reportError(error, "scheduler_loop");
      }
    }
  }

  private async drainDelayedJobs(): Promise<boolean> {
    const dueJobs = await this.redis.zrangebyscore(
      this.delayKey,
      "-inf",
      Date.now(),
      "LIMIT",
      0,
      this.batchSize,
    );

    if (dueJobs.length === 0) {
      return false;
    }

    let moved = false;

    for (const delayedJobId of dueJobs) {
      if (!this.running) {
        return moved;
      }

      moved = (await this.moveDelayedJob(delayedJobId)) || moved;
    }

    return moved;
  }

  private async moveDelayedJob(delayedJobId: string): Promise<boolean> {
    for (let attempt = 0; attempt < 3; attempt += 1) {
      await this.redis.watch(this.delayKey, this.jobKey);

      const score = await this.redis.zscore(this.delayKey, delayedJobId);
      const payload = await this.redis.hget(this.jobKey, delayedJobId);

      if (score === null || Number(score) > Date.now()) {
        await this.redis.unwatch();
        return false;
      }

      if (payload === null) {
        const cleanup = this.redis.multi();
        cleanup.zrem(this.delayKey, delayedJobId);
        const cleanupResult = await cleanup.exec();
        return cleanupResult !== null;
      }

      const transaction = this.redis.multi();
      transaction.zrem(this.delayKey, delayedJobId);
      transaction.xadd(
        this.streamKey,
        "MAXLEN",
        "~",
        this.streamMaxLen,
        "*",
        "jobId",
        delayedJobId,
      );

      const result = await transaction.exec();
      if (result !== null) {
        return true;
      }
    }

    return false;
  }

  private async reclaimPendingJobs(): Promise<number> {
    const result = await this.workerRedis.xautoclaim(
      this.streamKey,
      this.consumerGroup,
      this.consumerName,
      this.reclaimMinIdleMs,
      "0-0",
      "COUNT",
      this.reclaimBatchSize,
    );

    const entries = this.parseClaimedEntries(result);

    if (entries.length === 0) {
      return 0;
    }

    this.recordMetric("reclaimed", entries.length, "reclaim");

    for (const [streamEntryId, fields] of entries) {
      if (!this.running) {
        return 0;
      }

      const job = await this.getJobFromStreamEntry(fields);
      if (job === null) {
        await this.ackAndDeleteStreamEntry(streamEntryId);
        continue;
      }

      await this.processJob(streamEntryId, job);
    }

    return entries.length;
  }

  private parseClaimedEntries(raw: unknown): Array<[string, string[]]> {
    if (!Array.isArray(raw) || raw.length < 2) {
      return [];
    }

    const entries = raw[1];

    if (!Array.isArray(entries)) {
      return [];
    }

    return entries.filter((entry): entry is [string, string[]] => {
      if (!Array.isArray(entry) || entry.length !== 2) {
        return false;
      }

      return (
        typeof entry[0] === "string" &&
        Array.isArray(entry[1]) &&
        entry[1].every((field) => typeof field === "string")
      );
    });
  }

  private async getJobFromStreamEntry(
    fields: string[],
  ): Promise<StoredJob<Events> | null> {
    const jobIdIndex = fields.indexOf("jobId");

    if (jobIdIndex === -1 || jobIdIndex + 1 >= fields.length) {
      return null;
    }

    const jobId = fields[jobIdIndex + 1];

    if (jobId === undefined) {
      return null;
    }

    const payload = await this.redis.hget(this.jobKey, jobId);

    if (payload === null) {
      return null;
    }

    try {
      return JSON.parse(payload) as StoredJob<Events>;
    } catch {
      return null;
    }
  }

  private async processJob(
    streamEntryId: string,
    job: StoredJob<Events>,
  ): Promise<void> {
    const handler = this.handlers.get(job.event);

    if (handler === undefined) {
      const missingHandlerError = new Error(
        `No handler registered for event ${String(job.event)}`,
      );

      await this.deadLetterJob(streamEntryId, job, missingHandlerError);
      this.reportError(
        missingHandlerError,
        `handler_missing:${String(job.event)}`,
      );
      return;
    }

    try {
      await handler(job as Job<Events>);
      await this.finalizeJob(streamEntryId, job.id);
      this.recordMetric("processed", 1, "handler_success");
      this.onProcess?.(
        String(job.event),
        job as Job<QueueEvents, keyof QueueEvents>,
      );
    } catch (error) {
      await this.handleJobFailure(streamEntryId, job, error);
      this.reportError(error, `handler:${String(job.event)}`);
      return;
    }
  }

  private async handleJobFailure(
    streamEntryId: string,
    job: StoredJob<Events>,
    error: unknown,
  ): Promise<void> {
    const nextAttempts = job.attempts + 1;

    if (nextAttempts <= this.retryLimit) {
      const retryDelay = this.computeRetryDelay(nextAttempts);
      const retryAt = Date.now() + retryDelay;
      const nextJob: StoredJob<Events> = {
        ...job,
        attempts: nextAttempts,
        scheduledAt: retryAt,
      };

      const transaction = this.redis.multi();
      transaction.hset(this.jobKey, job.id, JSON.stringify(nextJob));
      transaction.zadd(this.delayKey, retryAt, job.id);
      transaction.xack(this.streamKey, this.consumerGroup, streamEntryId);
      transaction.xdel(this.streamKey, streamEntryId);
      const result = await transaction.exec();

      if (result === null) {
        throw new Error("Failed to schedule retry");
      }

      this.recordMetric("retried", 1, "handler_failure_retry");
      return;
    }

    const dlqPayload = JSON.stringify({
      ...job,
      attempts: nextAttempts,
      failedAt: Date.now(),
      error: this.normalizeErrorMessage(error),
    });

    const transaction = this.redis.multi();
    transaction.xadd(
      this.dlqKey,
      "MAXLEN",
      "~",
      this.dlqMaxLen,
      "*",
      "job",
      dlqPayload,
    );
    transaction.xack(this.streamKey, this.consumerGroup, streamEntryId);
    transaction.xdel(this.streamKey, streamEntryId);
    transaction.hdel(this.jobKey, job.id);
    const result = await transaction.exec();

    if (result === null) {
      throw new Error("Failed to move job to DLQ");
    }

    this.recordMetric("failed", 1, "handler_failure_terminal");
    this.recordMetric("deadLettered", 1, "handler_failure_terminal");
  }

  private computeRetryDelay(attempt: number): number {
    const exponential = this.retryBaseDelayMs * 2 ** Math.max(0, attempt - 1);
    const capped = Math.min(this.retryMaxDelayMs, exponential);

    if (this.retryJitterMs === 0) {
      return capped;
    }

    const jitter = Math.floor(Math.random() * (this.retryJitterMs + 1));
    return capped + jitter;
  }

  private normalizeErrorMessage(error: unknown): string {
    if (error instanceof Error) {
      return error.message;
    }

    return String(error);
  }

  private async finalizeJob(
    streamEntryId: string,
    jobId: string,
  ): Promise<void> {
    const transaction = this.redis.multi();
    transaction.xack(this.streamKey, this.consumerGroup, streamEntryId);
    transaction.xdel(this.streamKey, streamEntryId);
    transaction.hdel(this.jobKey, jobId);
    await transaction.exec();
  }

  private async deadLetterJob(
    streamEntryId: string,
    job: StoredJob<Events>,
    error: Error,
  ): Promise<void> {
    const dlqPayload = JSON.stringify({
      ...job,
      failedAt: Date.now(),
      error: error.message,
    });

    const transaction = this.redis.multi();
    transaction.xadd(
      this.dlqKey,
      "MAXLEN",
      "~",
      this.dlqMaxLen,
      "*",
      "job",
      dlqPayload,
    );
    transaction.xack(this.streamKey, this.consumerGroup, streamEntryId);
    transaction.xdel(this.streamKey, streamEntryId);
    transaction.hdel(this.jobKey, job.id);
    await transaction.exec();

    this.recordMetric("failed", 1, "dead_letter_missing_handler");
    this.recordMetric("deadLettered", 1, "dead_letter_missing_handler");
  }

  private async ackAndDeleteStreamEntry(streamEntryId: string): Promise<void> {
    const transaction = this.redis.multi();
    transaction.xack(this.streamKey, this.consumerGroup, streamEntryId);
    transaction.xdel(this.streamKey, streamEntryId);
    await transaction.exec();
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
  }

  private reportError(error: unknown, context: string): void {
    const normalized =
      error instanceof Error ? error : new Error(String(error));

    this.onError?.(normalized, context);
  }

  private recordMetric(
    name: keyof RedisQStats,
    value: number,
    context: string,
  ): void {
    this.stats[name] += value;

    this.onMetric?.({
      name,
      value,
      context,
      timestamp: Date.now(),
    });
  }
}

export type {
  Job,
  RedisQMetric,
  RedisQStats,
  QueueEvents,
  RedisQOptions,
} from "./types";
