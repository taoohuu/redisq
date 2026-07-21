import type Redis from "ioredis";

type QueueEvents = object;

type EventName<Events extends QueueEvents> = Extract<keyof Events, string>;

interface QueueOptions {
  delay?: number;
}

interface RedisQOptions {
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
  onError?: (error: Error, context: string) => void;
  onProcess?: (event: string, job: Job<QueueEvents, keyof QueueEvents>) => void;
  onMetric?: (metric: RedisQMetric) => void;
}

interface RedisQMetric {
  name: keyof RedisQStats;
  value: number;
  context: string;
  timestamp: number;
}

interface RedisQStats {
  added: number;
  cancelled: number;
  processed: number;
  retried: number;
  failed: number;
  deadLettered: number;
  reclaimed: number;
}

interface Job<
  Events extends QueueEvents,
  Event extends EventName<Events> = EventName<Events>,
> {
  id: string;
  event: Event;
  data: Events[Event];
  createdAt: number;
  scheduledAt: number;
  attempts: number;
}

export type {
  EventName,
  Job,
  QueueEvents,
  RedisQMetric,
  QueueOptions,
  RedisQOptions,
  RedisQStats,
};
