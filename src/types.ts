import type Redis from "ioredis";

type QueueEvents = object;

type EventName<Events extends QueueEvents> = Extract<keyof Events, string>;

type ProcessCallbackArgs<Events extends QueueEvents> = {
  [E in EventName<Events>]: [event: E, job: Job<Events, E>];
}[EventName<Events>];

type ProcessCallback<Events extends QueueEvents> = (
  ...args: ProcessCallbackArgs<Events>
) => void;

interface QueueOptions {
  delay?: number;
}

interface RedisQOptions<Events extends QueueEvents = QueueEvents> {
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
  onProcessStart?: ProcessCallback<Events>;
  onProcessEnd?: ProcessCallback<Events>;
  onError?: (error: Error, context: string) => void;
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
  QueueEvents,
  EventName,
  ProcessCallbackArgs,
  ProcessCallback,
  QueueOptions,
  RedisQOptions,
  RedisQMetric,
  RedisQStats,
  Job,
};
