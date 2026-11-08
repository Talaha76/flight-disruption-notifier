import type { Ctx } from './lib';

export interface ExperimentConfig {
  coldStartProfile?: string;
  processorBatchSize?: number;
  billingMode?: string;
  apiRateLimit?: number;
}

/** Deployed configuration, from the ExperimentConfig stack output (present after the Phase 3 deploy). */
export function experimentConfig(ctx: Ctx): ExperimentConfig {
  try {
    return JSON.parse(ctx.out.ExperimentConfig ?? '{}');
  } catch {
    return {};
  }
}
