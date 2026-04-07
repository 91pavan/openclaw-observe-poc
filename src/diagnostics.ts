/**
 * Diagnostic events integration — subscribes to OpenClaw's internal diagnostic
 * events to get accurate cost/token data, then enriches our connected traces.
 *
 * This combines the best of both approaches:
 * - Our plugin: Connected traces (request → agent turn → tools)
 * - Official diagnostics: Accurate cost, token counts, context limits
 */

import type { Span } from "@opentelemetry/api";
import type { TelemetryRuntime } from "./telemetry.js";

// Import from OpenClaw plugin SDK (loaded lazily)
let onDiagnosticEvent: ((listener: (evt: any) => void) => () => void) | null = null;
let sdkLoadAttempted = false;

async function loadSdk(): Promise<void> {
  if (sdkLoadAttempted) return;
  sdkLoadAttempted = true;
  try {
    // Dynamic import to avoid build issues if SDK not available
    // @ts-ignore - openclaw/plugin-sdk types not available at build time
    const sdk = await import("openclaw/plugin-sdk") as any;
    onDiagnosticEvent = sdk.onDiagnosticEvent;
  } catch {
    // SDK not available — will use fallback token extraction
  }
}

/** Pending usage data waiting to be attached to spans */
interface PendingUsageData {
  costUsd?: number;
  usage: {
    input?: number;
    output?: number;
    cacheRead?: number;
    cacheWrite?: number;
    total?: number;
  };
  context?: {
    limit?: number;
    used?: number;
  };
  durationMs?: number;
  provider?: string;
  model?: string;
}

function firstNumber(...values: unknown[]): number | undefined {
  for (const value of values) {
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
    if (typeof value === "string") {
      const trimmed = value.trim();
      if (!trimmed) continue;
      const parsed = Number(trimmed);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
  }
  return undefined;
}

function firstString(...values: unknown[]): string | undefined {
  for (const value of values) {
    if (typeof value !== "string") continue;
    const trimmed = value.trim();
    if (trimmed) {
      return trimmed;
    }
  }
  return undefined;
}

function resolveDiagnosticSessionKey(evt: any): string {
  return firstString(
    evt?.sessionKey,
    evt?.conversationId,
    evt?.metadata?.sessionKey,
    evt?.metadata?.conversationId,
    evt?.context?.sessionKey,
    evt?.context?.conversationId
  ) || "unknown";
}

function normalizeUsageData(rawUsage: any): PendingUsageData["usage"] {
  const usage = rawUsage || {};
  const metadata = usage.usageMetadata || usage.metadata || {};
  const input = firstNumber(
    usage.input,
    usage.inputTokens,
    usage.input_tokens,
    usage.prompt,
    usage.promptTokens,
    usage.prompt_tokens,
    usage.promptTokenCount,
    metadata.input,
    metadata.inputTokens,
    metadata.input_tokens,
    metadata.prompt,
    metadata.promptTokens,
    metadata.prompt_tokens,
    metadata.promptTokenCount
  );
  const output = firstNumber(
    usage.output,
    usage.outputTokens,
    usage.output_tokens,
    usage.completion,
    usage.completionTokens,
    usage.completion_tokens,
    usage.candidatesTokenCount,
    usage.outputTokenCount,
    metadata.output,
    metadata.outputTokens,
    metadata.output_tokens,
    metadata.completion,
    metadata.completionTokens,
    metadata.completion_tokens,
    metadata.candidatesTokenCount,
    metadata.outputTokenCount
  );
  const cacheRead = firstNumber(
    usage.cacheRead,
    usage.cache_read,
    usage.cacheReadTokens,
    usage.cache_read_tokens,
    usage.cachedContentTokenCount,
    metadata.cacheRead,
    metadata.cache_read,
    metadata.cacheReadTokens,
    metadata.cache_read_tokens,
    metadata.cachedContentTokenCount
  );
  const cacheWrite = firstNumber(
    usage.cacheWrite,
    usage.cache_write,
    usage.cacheCreation,
    usage.cacheCreationInputTokens,
    usage.cache_creation_input_tokens,
    usage.cacheWriteTokens,
    usage.cache_write_tokens,
    metadata.cacheWrite,
    metadata.cache_write,
    metadata.cacheCreation,
    metadata.cacheCreationInputTokens,
    metadata.cache_creation_input_tokens,
    metadata.cacheWriteTokens,
    metadata.cache_write_tokens
  );
  const total = firstNumber(
    usage.total,
    usage.totalTokens,
    usage.total_tokens,
    usage.totalTokenCount,
    metadata.total,
    metadata.totalTokens,
    metadata.total_tokens,
    metadata.totalTokenCount,
    input !== undefined || output !== undefined || cacheRead !== undefined || cacheWrite !== undefined
      ? (input || 0) + (output || 0) + (cacheRead || 0) + (cacheWrite || 0)
      : undefined
  );

  return { input, output, cacheRead, cacheWrite, total };
}

function summarizeDiagnosticShape(evt: any): Record<string, unknown> {
  const usage = evt?.usage || {};
  const metadata = usage?.usageMetadata || usage?.metadata || {};

  return {
    topLevelKeys: Object.keys(evt || {}).sort(),
    usageKeys: Object.keys(usage).sort(),
    usageMetadataKeys: Object.keys(metadata).sort(),
    sessionCandidates: {
      sessionKey: evt?.sessionKey,
      conversationId: evt?.conversationId,
      contextSessionKey: evt?.context?.sessionKey,
      contextConversationId: evt?.context?.conversationId,
    },
    modelCandidates: {
      model: evt?.model,
      modelName: evt?.modelName,
      provider: evt?.provider,
      vendor: evt?.vendor,
      system: evt?.system,
    },
    tokenCandidates: {
      usageInput: usage?.input,
      usageOutput: usage?.output,
      usageTotal: usage?.total,
      usageInputTokens: usage?.inputTokens,
      usageOutputTokens: usage?.outputTokens,
      usageTotalTokens: usage?.totalTokens,
      usagePromptTokenCount: usage?.promptTokenCount,
      usageCandidatesTokenCount: usage?.candidatesTokenCount,
      usageTotalTokenCount: usage?.totalTokenCount,
      metadataPromptTokenCount: metadata?.promptTokenCount,
      metadataCandidatesTokenCount: metadata?.candidatesTokenCount,
      metadataTotalTokenCount: metadata?.totalTokenCount,
    },
  };
}

/** Map of sessionKey → pending usage data from diagnostic events */
const pendingUsageMap = new Map<string, PendingUsageData>();

/** Map of sessionKey → active agent span (set by hooks.ts) */
export const activeAgentSpans = new Map<string, Span>();

/**
 * Register diagnostic event listener to capture model.usage events.
 * Returns unsubscribe function.
 */
export async function registerDiagnosticsListener(
  telemetry: TelemetryRuntime,
  logger: any
): Promise<() => void> {
  // Load the SDK if not already loaded
  await loadSdk();

  if (!onDiagnosticEvent) {
    logger.debug("[otel] onDiagnosticEvent not available — using fallback token extraction");
    return () => {};
  }

  const { counters, histograms } = telemetry;

  const unsubscribe = onDiagnosticEvent((evt: any) => {
    if (evt.type !== "model.usage") return;

    const sessionKey = resolveDiagnosticSessionKey(evt);
    const usage = normalizeUsageData(evt.usage);
    const costUsd = evt.costUsd;
    const model = firstString(evt.model, evt.modelName) || "unknown";
    const provider = firstString(evt.provider, evt.vendor, evt.system) || "unknown";
    const pendingUsage: PendingUsageData = {
      costUsd,
      usage,
      context: evt.context,
      durationMs: evt.durationMs,
      provider,
      model,
    };

    // Store for later attachment to agent span
    pendingUsageMap.set(sessionKey, pendingUsage);

    // Record metrics immediately (don't wait for span)
    const metricAttrs = {
      "gen_ai.response.model": model,
      "openclaw.provider": provider,
    };

    if (usage.input !== undefined) {
      counters.tokensPrompt.add(usage.input, metricAttrs);
    }
    if (usage.output !== undefined) {
      counters.tokensCompletion.add(usage.output, metricAttrs);
    }
    if (usage.cacheRead !== undefined) {
      counters.tokensPrompt.add(usage.cacheRead, { ...metricAttrs, "token.type": "cache_read" });
    }
    if (usage.cacheWrite !== undefined) {
      counters.tokensPrompt.add(usage.cacheWrite, { ...metricAttrs, "token.type": "cache_write" });
    }
    if (usage.total !== undefined) {
      counters.tokensTotal.add(usage.total, metricAttrs);
    }

    if (model !== "unknown" && usage.input === undefined && usage.output === undefined && usage.total === undefined) {
      logger.debug(`[otel] model.usage unresolved token shape: ${JSON.stringify(summarizeDiagnosticShape(evt))}`);
    }

    // Record cost metric
    if (typeof costUsd === "number" && costUsd > 0) {
      telemetry.meter.createCounter("openclaw.llm.cost.usd", {
        description: "Estimated LLM cost in USD",
        unit: "usd",
      }).add(costUsd, metricAttrs);
    }

    // Record LLM duration
    if (typeof evt.durationMs === "number") {
      histograms.llmDuration.record(evt.durationMs, metricAttrs);
    }

    counters.llmRequests.add(1, metricAttrs);

    // If we have an active agent span for this session, enrich it now
    const agentSpan = activeAgentSpans.get(sessionKey);
    if (agentSpan) {
      enrichSpanWithUsage(agentSpan, pendingUsage);
      pendingUsageMap.delete(sessionKey);
    }

    logger.debug(`[otel] model.usage: session=${sessionKey}, model=${model}, cost=$${costUsd?.toFixed(4) || "?"}, tokens=${usage.total || "?"}`);
  });

  logger.info("[otel] Subscribed to OpenClaw diagnostic events (model.usage, etc.)");
  return unsubscribe;
}

/**
 * Get pending usage data for a session (if any).
 * Called by agent_end hook to attach data to span.
 */
export function getPendingUsage(sessionKey: string): PendingUsageData | undefined {
  const data = pendingUsageMap.get(sessionKey);
  if (data) {
    pendingUsageMap.delete(sessionKey);
  }
  return data;
}

/**
 * Enrich a span with usage data from diagnostic event.
 */
export function enrichSpanWithUsage(span: Span, data: PendingUsageData): void {
  const usage = data.usage || {};

  // GenAI semantic convention attributes
  if (usage.input !== undefined) {
    span.setAttribute("gen_ai.usage.input_tokens", usage.input);
  }
  if (usage.output !== undefined) {
    span.setAttribute("gen_ai.usage.output_tokens", usage.output);
  }
  if (usage.total !== undefined) {
    span.setAttribute("gen_ai.usage.total_tokens", usage.total);
  }
  if (usage.cacheRead !== undefined) {
    span.setAttribute("gen_ai.usage.cache_read_tokens", usage.cacheRead);
  }
  if (usage.cacheWrite !== undefined) {
    span.setAttribute("gen_ai.usage.cache_write_tokens", usage.cacheWrite);
  }

  // Cost (custom attribute — not in GenAI semconv yet)
  if (data.costUsd !== undefined) {
    span.setAttribute("openclaw.llm.cost_usd", data.costUsd);
  }

  // Context window
  if (data.context?.limit !== undefined) {
    span.setAttribute("openclaw.context.limit", data.context.limit);
  }
  if (data.context?.used !== undefined) {
    span.setAttribute("openclaw.context.used", data.context.used);
  }

  // Provider/model
  if (data.provider) {
    span.setAttribute("gen_ai.system", data.provider);
  }
  if (data.model) {
    span.setAttribute("gen_ai.response.model", data.model);
  }
}

/**
 * Check if diagnostic events are available.
 * Note: Only accurate after registerDiagnosticsListener() has been called.
 */
export function hasDiagnosticsSupport(): boolean {
  return onDiagnosticEvent !== null;
}

/**
 * Async check for diagnostics support (loads SDK if needed).
 */
export async function checkDiagnosticsSupport(): Promise<boolean> {
  await loadSdk();
  return onDiagnosticEvent !== null;
}
