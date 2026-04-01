/**
 * OpenClaw event hooks — captures tool executions, agent turns, messages,
 * and gateway lifecycle as connected OTel traces.
 *
 * Trace structure per request:
 *   openclaw.request (root span, covers full message → reply lifecycle)
 *   ├── openclaw.agent.turn (agent processing span)
 *   │   ├── llm.claude-sonnet-4 (LLM call)
 *   │   ├── tool.exec (tool call)          ← fork/join detected here
 *   │   ├── tool.Read (tool call)
 *   │   ├── anthropic.chat (auto-instrumented by OpenLLMetry)
 *   │   └── tool.write (tool call)
 *   └── openclaw.message.sent
 *
 * Context propagation:
 *   - message_received: creates root span, stores in sessionContextMap
 *   - message_sent: records final outbound delivery under the active request
 *   - before_agent_start: creates child "agent turn" span under root
 *     + agent handoff tracking via span links
 *     + join detection from previous parallel fork
 *   - llm_input/llm_output: create and complete explicit LLM spans
 *   - before_tool_call/after_tool_call: create and complete explicit tool spans
 *     + fork detection for parallel tool calls
 *   - tool_result_persist: fallback tool span when lifecycle hooks are unavailable
 *   - agent_end: ends the agent turn span
 *     + finalizes fork groups, annotates join metadata
 *
 * IMPORTANT: OpenClaw has TWO hook registration systems:
 *   - api.registerHook() → event-stream hooks (command:new, gateway:startup)
 *   - api.on()           → typed plugin hooks (tool_result_persist, agent_end)
 */

import { SpanKind, SpanStatusCode, context, trace, type Span, type Context, type Link } from "@opentelemetry/api";
import type { TelemetryRuntime } from "./telemetry.js";
import type { OtelObservabilityConfig } from "./config.js";
import { activeAgentSpans, getPendingUsage } from "./diagnostics.js";
import { checkToolSecurity, checkMessageSecurity, type SecurityCounters } from "./security.js";
import { onAgentStart, onAgentEnd, cleanupHandoff, getHandoffSequence, setHandoffLogger } from "./handoff.js";
import { registerToolSpan, finalizeAgentTurn, consumeJoin, cleanupForkJoin, setForkJoinLogger } from "./forkjoin.js";
import {
  ObserveSpanKind,
  ATTR_OBSERVE_SPAN_KIND,
  ATTR_OBSERVE_ENTITY_NAME,
  ATTR_OBSERVE_ENTITY_INPUT,
  ATTR_OBSERVE_ENTITY_OUTPUT,
} from "./observe-attributes.js";
import { touchSession, endSession, removeSession } from "./session-lifecycle.js";

/** Active trace context for a session — allows connecting spans into one trace. */
interface ToolSpanContext {
  span: Span;
  toolName: string;
  toolCallId?: string;
}

interface SessionTraceContext {
  rootSpan: Span;
  rootContext: Context;
  agentSpan?: Span;
  agentContext?: Context;
  llmSpan?: Span;
  toolSpans: Map<string, ToolSpanContext>;
  toolSpanSequence: number;
  rootEnded?: boolean;
  agentEnded?: boolean;
  startTime: number;
}

/** Map of sessionKey → active trace context. Cleaned up on agent_end. */
const sessionContextMap = new Map<string, SessionTraceContext>();
const recentToolCompletions = new Map<string, number>();
const UNKNOWN_SESSION_KEY = "unknown";

function normalizeString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function firstString(...values: unknown[]): string | undefined {
  for (const value of values) {
    const normalized = normalizeString(value);
    if (normalized) {
      return normalized;
    }
  }

  return undefined;
}

function isUnknownSessionKey(sessionKey: string | undefined): boolean {
  return !sessionKey || sessionKey === UNKNOWN_SESSION_KEY;
}

function ensureStandaloneSessionContext(
  tracer: TelemetryRuntime["tracer"],
  sessionKey: string
): SessionTraceContext {
  const existing = sessionContextMap.get(sessionKey);
  if (existing) {
    return existing;
  }

  const rootSpan = tracer.startSpan("openclaw.request", {
    kind: SpanKind.INTERNAL,
    attributes: {
      [ATTR_OBSERVE_SPAN_KIND]: ObserveSpanKind.WORKFLOW,
      [ATTR_OBSERVE_ENTITY_NAME]: "openclaw.request",
      "openclaw.message.channel": "unknown",
      "openclaw.session.key": sessionKey,
      "openclaw.message.direction": "unknown",
      "openclaw.message.from": "unknown",
    },
  });
  const rootContext = trace.setSpan(context.active(), rootSpan);
  const sessionCtx: SessionTraceContext = {
    rootSpan,
    rootContext,
    toolSpans: new Map<string, ToolSpanContext>(),
    toolSpanSequence: 0,
    startTime: Date.now(),
  };
  sessionContextMap.set(sessionKey, sessionCtx);
  return sessionCtx;
}

function getParentContext(sessionCtx: SessionTraceContext | undefined): Context {
  return sessionCtx?.agentContext || sessionCtx?.rootContext || context.active();
}

function resolveSessionKey(event: any, ctx: any): string {
  return (
    firstString(
      event?.sessionKey,
      ctx?.sessionKey,
      event?.sessionId,
      ctx?.sessionId,
      event?.conversationId,
      ctx?.conversationId,
      event?.conversation?.id,
      ctx?.conversation?.id,
      event?.threadId,
      ctx?.threadId,
      event?.thread?.id,
      ctx?.thread?.id,
      event?.metadata?.sessionKey,
      ctx?.metadata?.sessionKey,
      event?.metadata?.sessionId,
      ctx?.metadata?.sessionId,
      event?.metadata?.conversationId,
      ctx?.metadata?.conversationId,
      event?.metadata?.threadId,
      ctx?.metadata?.threadId,
      event?.message?.sessionKey,
      ctx?.message?.sessionKey,
      event?.message?.sessionId,
      ctx?.message?.sessionId,
      event?.message?.conversationId,
      ctx?.message?.conversationId,
      event?.body?.sessionKey,
      ctx?.body?.sessionKey,
      event?.body?.conversationId,
      ctx?.body?.conversationId
    ) || UNKNOWN_SESSION_KEY
  );
}

function resolveChannel(event: any, ctx: any): string {
  return event?.channel || event?.channelId || ctx?.channelId || ctx?.messageProvider || "unknown";
}

function resolveSender(event: any, ctx: any): string {
  return (
    event?.from ||
    event?.senderId ||
    event?.metadata?.senderId ||
    event?.metadata?.from ||
    ctx?.accountId ||
    "unknown"
  );
}

function resolveRecipient(event: any, ctx: any): string {
  return event?.to || event?.recipient || event?.metadata?.to || ctx?.accountId || "unknown";
}

function stringifyContent(value: unknown): string | undefined {
  if (typeof value === "string") {
    return value;
  }
  if (Array.isArray(value)) {
    const parts = value
      .map((item) => {
        if (typeof item === "string") {
          return item;
        }
        if (
          item &&
          typeof item === "object" &&
          "text" in item &&
          typeof (item as { text?: unknown }).text === "string"
        ) {
          return (item as { text: string }).text;
        }
        return undefined;
      })
      .filter((item): item is string => typeof item === "string" && item.length > 0);
    return parts.length > 0 ? parts.join("\n\n") : undefined;
  }
  return undefined;
}

function extractLlmOutputText(event: any): string | undefined {
  return (
    stringifyContent(event?.assistantTexts) ||
    stringifyContent(event?.output) ||
    stringifyContent(event?.content) ||
    stringifyContent(event?.lastAssistant?.content)
  );
}

function extractInboundMessageText(event: any): string {
  return stringifyContent(event?.content) || event?.text || event?.message || event?.body || event?.bodyForAgent || "";
}

function endRootSpan(sessionCtx: SessionTraceContext | undefined, errorMsg?: unknown): void {
  if (!sessionCtx || sessionCtx.rootEnded) {
    return;
  }

  const totalMs = Date.now() - sessionCtx.startTime;
  sessionCtx.rootSpan.setAttribute("openclaw.request.duration_ms", totalMs);
  if (errorMsg) {
    sessionCtx.rootSpan.setStatus({ code: SpanStatusCode.ERROR, message: String(errorMsg).slice(0, 200) });
  } else {
    sessionCtx.rootSpan.setStatus({ code: SpanStatusCode.OK });
  }
  sessionCtx.rootSpan.end();
  sessionCtx.rootEnded = true;
}

function adoptPendingUnknownSession(
  sessionKey: string,
  logger: any
): SessionTraceContext | undefined {
  if (isUnknownSessionKey(sessionKey)) {
    return sessionContextMap.get(UNKNOWN_SESSION_KEY);
  }

  const existing = sessionContextMap.get(sessionKey);
  if (existing) {
    return existing;
  }

  const unknownSession = sessionContextMap.get(UNKNOWN_SESSION_KEY);
  if (!unknownSession || unknownSession.agentSpan || unknownSession.rootEnded) {
    return undefined;
  }

  sessionContextMap.delete(UNKNOWN_SESSION_KEY);
  unknownSession.rootSpan.setAttribute("openclaw.session.key", sessionKey);
  sessionContextMap.set(sessionKey, unknownSession);

  removeSession(UNKNOWN_SESSION_KEY);
  touchSession(sessionKey, unknownSession.rootContext);

  logger.info?.(
    `[otel] Reconciled inbound request session: ${UNKNOWN_SESSION_KEY} -> ${sessionKey}`
  );

  return unknownSession;
}

function getOrCreateSessionContext(
  tracer: TelemetryRuntime["tracer"],
  sessionKey: string,
  logger: any
): SessionTraceContext {
  const adopted = adoptPendingUnknownSession(sessionKey, logger);
  if (adopted) {
    return adopted;
  }

  return ensureStandaloneSessionContext(tracer, sessionKey);
}

function getToolSpanKey(params: {
  sessionCtx: SessionTraceContext;
  sessionKey: string;
  toolName: string;
  toolCallId?: string;
}): string {
  if (params.toolCallId) {
    return `session:${params.sessionKey}:toolcall:${params.toolCallId}`;
  }

  params.sessionCtx.toolSpanSequence += 1;
  return `session:${params.sessionKey}:${params.toolName}:${params.sessionCtx.toolSpanSequence}`;
}

function findToolSpanEntry(params: {
  sessionCtx: SessionTraceContext | undefined;
  sessionKey: string;
  toolName: string;
  toolCallId?: string;
}): [string, ToolSpanContext] | undefined {
  const { sessionCtx, sessionKey, toolName, toolCallId } = params;
  if (!sessionCtx) {
    return undefined;
  }

  if (toolCallId) {
    const exactKey = `session:${sessionKey}:toolcall:${toolCallId}`;
    const exactMatch = sessionCtx.toolSpans.get(exactKey);
    if (exactMatch) {
      return [exactKey, exactMatch];
    }
  }

  const matches = Array.from(sessionCtx.toolSpans.entries()).filter(([key, value]) => {
    return key.startsWith(`session:${sessionKey}:${toolName}:`) || value.toolName === toolName;
  });

  return matches.at(-1);
}

function rememberToolCompletion(key: string): void {
  recentToolCompletions.set(key, Date.now());
}

function wasToolRecentlyCompleted(key: string): boolean {
  const completedAt = recentToolCompletions.get(key);
  return typeof completedAt === "number" && Date.now() - completedAt < 10_000;
}

function completeToolSpan(params: {
  sessionCtx: SessionTraceContext | undefined;
  sessionKey: string;
  toolName: string;
  toolCallId?: string;
  durationMs?: unknown;
  error?: unknown;
  result?: unknown;
  logger: any;
}): void {
  const matched = findToolSpanEntry(params);
  if (!matched) {
    return;
  }

  const [spanKey, toolSpanCtx] = matched;
  const { span } = toolSpanCtx;
  const durationMs = typeof params.durationMs === "number" ? params.durationMs : undefined;

  if (durationMs !== undefined) {
    span.setAttribute("openclaw.tool.duration_ms", durationMs);
  }

  if (params.result !== undefined) {
    const resultText = typeof params.result === "string" ? params.result : JSON.stringify(params.result);
    if (typeof resultText === "string") {
      span.setAttribute("openclaw.tool.result_chars", resultText.length);
      if (resultText.length > 0) {
        span.setAttribute(ATTR_OBSERVE_ENTITY_OUTPUT, resultText.slice(0, 4096));
      }
    }
  }

  if (params.error) {
    span.setAttribute("openclaw.tool.error", String(params.error).slice(0, 500));
    span.setStatus({ code: SpanStatusCode.ERROR, message: String(params.error).slice(0, 200) });
  } else {
    span.setStatus({ code: SpanStatusCode.OK });
  }

  span.end();
  params.sessionCtx?.toolSpans.delete(spanKey);
  rememberToolCompletion(spanKey);
  rememberToolCompletion(`session:${params.sessionKey}:tool:${params.toolName}`);
  params.logger.debug?.(`[otel] Tool span completed: session=${params.sessionKey} tool=${params.toolName}`);
}

/**
 * Register all plugin hooks on the OpenClaw plugin API.
 */
export function registerHooks(
  api: any,
  telemetry: TelemetryRuntime,
  config: OtelObservabilityConfig
): void {
  const { tracer, counters, histograms } = telemetry;
  const logger = api.logger;
  // Initialize loggers for sub-modules
  setHandoffLogger(logger);
  setForkJoinLogger(logger);
  // ═══════════════════════════════════════════════════════════════════
  // TYPED HOOKS — registered via api.on() into registry.typedHooks
  // ═══════════════════════════════════════════════════════════════════

  // ── message_received ─────────────────────────────────────────────
  // Creates the ROOT span for the entire request lifecycle.
  // All subsequent spans (agent, tools) become children of this span.

  // Build security counters object for detection module
  const securityCounters: SecurityCounters = {
    securityEvents: counters.securityEvents,
    sensitiveFileAccess: counters.sensitiveFileAccess,
    promptInjection: counters.promptInjection,
    dangerousCommand: counters.dangerousCommand,
  };

  api.on(
    "message_received",
    async (event: any, ctx: any) => {
      try {
        const channel = resolveChannel(event, ctx);
        const sessionKey = resolveSessionKey(event, ctx);
        const from = resolveSender(event, ctx);
        const messageText = extractInboundMessageText(event);

        logger.debug?.(
          `[otel] message_received resolved session=${sessionKey}, channel=${channel}, from=${from}`
        );

        // Create root span for this request
        const rootSpan = tracer.startSpan("openclaw.request", {
          kind: SpanKind.SERVER,
          attributes: {
            [ATTR_OBSERVE_SPAN_KIND]: ObserveSpanKind.WORKFLOW,
            [ATTR_OBSERVE_ENTITY_NAME]: "openclaw.request",
            "openclaw.message.channel": channel,
            "openclaw.session.key": sessionKey,
            "openclaw.message.direction": "inbound",
            "openclaw.message.from": from,
          },
        });

        // ═══ SECURITY DETECTION 2: Prompt Injection ═══════════════
        if (messageText && typeof messageText === "string" && messageText.length > 0) {
          const securityEvent = checkMessageSecurity(
            messageText,
            rootSpan,
            securityCounters,
            sessionKey
          );
          if (securityEvent) {
            logger.warn?.(`[otel] SECURITY: ${securityEvent.detection} - ${securityEvent.description}`);
          }
        }

        // Store the context so child spans can reference it
        const rootContext = trace.setSpan(context.active(), rootSpan);

        sessionContextMap.set(sessionKey, {
          rootSpan,
          rootContext,
          toolSpans: new Map<string, ToolSpanContext>(),
          toolSpanSequence: 0,
          startTime: Date.now(),
        });

        // Track session activity for lifecycle management
        touchSession(sessionKey, rootContext);

        // Capture message content if configured
        if (config.captureContent && messageText) {
          rootSpan.setAttribute(ATTR_OBSERVE_ENTITY_INPUT, String(messageText).slice(0, 4096));
        }

        // Record message count metric
        counters.messagesReceived.add(1, {
          "openclaw.message.channel": channel,
        });

        logger.info?.(`[otel] Root span started for session=${sessionKey}, channel=${channel}`);
      } catch {
        // Never let telemetry errors break the main flow
      }
    },
    { priority: 100 } // High priority — run first to establish context
  );

  logger.info("[otel] Registered message_received hook (via api.on)");

  // ── message_sent ────────────────────────────────────────────────
  // Records the final outbound delivery under the active request trace.

  api.on(
    "message_sent",
    (event: any, ctx: any) => {
      try {
        const sessionKey = resolveSessionKey(event, ctx);
        const channel = resolveChannel(event, ctx);
        const to = resolveRecipient(event, ctx);
        const success = event?.success !== false;
        const messageText = stringifyContent(event?.content) || stringifyContent(event?.message);
        const sessionCtx = sessionContextMap.get(sessionKey);
        const parentContext = getParentContext(sessionCtx);

        counters.messagesSent.add(1, {
          "openclaw.message.channel": channel,
        });

        const span = tracer.startSpan(
          "openclaw.message.sent",
          {
            kind: SpanKind.PRODUCER,
            attributes: {
              [ATTR_OBSERVE_SPAN_KIND]: ObserveSpanKind.TASK,
              [ATTR_OBSERVE_ENTITY_NAME]: "openclaw.message.sent",
              "openclaw.message.channel": channel,
              "openclaw.message.direction": "outbound",
              "openclaw.message.to": to,
              "openclaw.message.success": success,
              "openclaw.session.key": sessionKey,
            },
          },
          parentContext
        );

        if (config.captureContent && messageText) {
          span.setAttribute(ATTR_OBSERVE_ENTITY_OUTPUT, messageText.slice(0, 4096));
        } else if (messageText) {
          span.setAttribute("openclaw.message.content_chars", messageText.length);
        }

        if (event?.error) {
          span.setAttribute("openclaw.message.error", String(event.error).slice(0, 500));
          span.setStatus({ code: SpanStatusCode.ERROR, message: String(event.error).slice(0, 200) });
        } else {
          span.setStatus({ code: success ? SpanStatusCode.OK : SpanStatusCode.ERROR });
        }

        touchSession(sessionKey, parentContext);
        span.end();

        // Current OpenClaw builds may not emit before_agent_start/agent_end consistently.
        // If no agent span was established for this request, finalize the root span here
        // so the backend receives a complete trace instead of waiting for stale cleanup.
        if (sessionCtx && !sessionCtx.agentSpan) {
          endRootSpan(sessionCtx, event?.error);
          sessionContextMap.delete(sessionKey);
        }
      } catch {
        // Never let telemetry errors break the main flow
      }

      return undefined;
    },
    { priority: -95 }
  );

  logger.info("[otel] Registered message_sent hook (via api.on)");

  // ── before_agent_start ───────────────────────────────────────────
  // Creates an "agent turn" child span under the root request span.

  api.on(
    "before_agent_start",
    (event: any, ctx: any) => {
      try {
        const sessionKey = resolveSessionKey(event, ctx);
        const agentId = event?.agentId || ctx?.agentId || "unknown";
        const model = event?.model || "unknown";

        const sessionCtx = getOrCreateSessionContext(tracer, sessionKey, logger);
        const parentContext = sessionCtx.rootContext;

        logger.debug?.(
          `[otel] before_agent_start resolved session=${sessionKey}, agent=${agentId}, model=${model}`
        );

        // Check for join from a previous parallel fork
        const joinInfo = consumeJoin(sessionKey);
        const joinLinks: Link[] = joinInfo?.links ?? [];
        if (joinInfo) {
          logger.info?.(
            `[otel] Join detected for agent=${agentId}: forkId=${joinInfo.attributes["ioa_observe.join.fork_id"]}, ` +
            `branches=${joinInfo.attributes["ioa_observe.join.branch_count"]}`
          );
        }

        // Create agent turn span as child of root span
        const agentSpan = tracer.startSpan(
          "openclaw.agent.turn",
          {
            kind: SpanKind.INTERNAL,
            attributes: {
              [ATTR_OBSERVE_SPAN_KIND]: ObserveSpanKind.AGENT,
              [ATTR_OBSERVE_ENTITY_NAME]: agentId,
              "openclaw.agent.id": agentId,
              "openclaw.session.key": sessionKey,
              "openclaw.agent.model": model,
            },
            links: joinLinks,
          },
          parentContext
        );

        // Annotate join metadata if this agent follows a fork
        if (joinInfo) {
          for (const [k, v] of Object.entries(joinInfo.attributes)) {
            agentSpan.setAttribute(k, v);
          }
          agentSpan.addEvent("agent.join", {
            "ioa_observe.join.fork_id": joinInfo.attributes["ioa_observe.join.fork_id"],
            "ioa_observe.join.branch_count": joinInfo.attributes["ioa_observe.join.branch_count"],
          });
        }

        // Agent handoff tracking — link back to previous agent
        const handoff = onAgentStart(sessionKey, agentId, agentSpan);
        for (const [k, v] of Object.entries(handoff.attributes)) {
          agentSpan.setAttribute(k, v);
        }
        if (handoff.links.length > 0) {
          logger.debug?.(
            `[otel] Handoff links prepared for agent=${agentId}: ${handoff.links.length} link(s), ` +
            `seq=${handoff.attributes["ioa_observe.agent.sequence"]}, ` +
            `previous=${handoff.attributes["ioa_observe.agent.previous"] || "(none)"}`
          );
        }
        // Note: span links cannot be added after creation in OTel JS,
        // so handoff links will apply to the NEXT agent span via onAgentStart

        const agentContext = trace.setSpan(parentContext, agentSpan);

        // Store agent span context for tool spans
        sessionCtx.agentSpan = agentSpan;
        sessionCtx.agentContext = agentContext;

        // Register in activeAgentSpans for diagnostics integration
        activeAgentSpans.set(sessionKey, agentSpan);

        logger.info?.(`[otel] Agent turn started: agent=${agentId}, model=${model}, session=${sessionKey}`);
      } catch {
        // Silently ignore
      }

      // Return undefined — don't modify system prompt
      return undefined;
    },
    { priority: 90 }
  );

  logger.info("[otel] Registered before_agent_start hook (via api.on)");

  // ── llm_input ───────────────────────────────────────────────────
  // Starts an explicit LLM span beneath the active agent/request context.

  api.on(
    "llm_input",
    (event: any, ctx: any) => {
      try {
        const sessionKey = ctx?.sessionKey || event?.sessionKey;
        if (!sessionKey) {
          return undefined;
        }

        const sessionCtx = getOrCreateSessionContext(tracer, sessionKey, logger);
        const parentContext = getParentContext(sessionCtx);

        if (sessionCtx.llmSpan) {
          sessionCtx.llmSpan.end();
          sessionCtx.llmSpan = undefined;
        }

        const model = event?.model || "unknown";
        const provider = event?.provider || "unknown";
        const llmSpan = tracer.startSpan(
          `llm.${model}`,
          {
            kind: SpanKind.INTERNAL,
            attributes: {
              [ATTR_OBSERVE_SPAN_KIND]: ObserveSpanKind.TASK,
              [ATTR_OBSERVE_ENTITY_NAME]: model,
              "gen_ai.response.model": model,
              "gen_ai.system": provider,
              "openclaw.session.key": sessionKey,
              "openclaw.agent.id": ctx?.agentId || "unknown",
              "openclaw.llm.images_count": typeof event?.imagesCount === "number" ? event.imagesCount : 0,
            },
          },
          parentContext
        );

        const promptText = stringifyContent(event?.prompt);
        if (config.captureContent && promptText) {
          llmSpan.setAttribute(ATTR_OBSERVE_ENTITY_INPUT, promptText.slice(0, 4096));
        }

        sessionCtx.llmSpan = llmSpan;
        touchSession(sessionKey, parentContext);
      } catch {
        // Never let telemetry errors break the main flow
      }

      return undefined;
    },
    { priority: 85 }
  );

  logger.info("[otel] Registered llm_input hook (via api.on)");

  // ── llm_output ──────────────────────────────────────────────────
  // Completes the explicit LLM span with output and usage attributes.

  api.on(
    "llm_output",
    (event: any, ctx: any) => {
      try {
        const sessionKey = ctx?.sessionKey || event?.sessionKey;
        if (!sessionKey) {
          return undefined;
        }

        const sessionCtx = sessionContextMap.get(sessionKey);
        const llmSpan = sessionCtx?.llmSpan;
        if (!llmSpan) {
          return undefined;
        }

        const usage = event?.usage || {};
        const model = event?.model || "unknown";
        const provider = event?.provider || "unknown";
        const outputText = extractLlmOutputText(event);

        llmSpan.setAttribute("gen_ai.response.model", model);
        llmSpan.setAttribute("gen_ai.system", provider);

        if (typeof usage.input === "number") {
          llmSpan.setAttribute("gen_ai.usage.input_tokens", usage.input);
        }
        if (typeof usage.output === "number") {
          llmSpan.setAttribute("gen_ai.usage.output_tokens", usage.output);
        }
        if (typeof usage.total === "number") {
          llmSpan.setAttribute("gen_ai.usage.total_tokens", usage.total);
        }
        if (typeof usage.cacheRead === "number") {
          llmSpan.setAttribute("gen_ai.usage.cache_read_tokens", usage.cacheRead);
        }
        if (typeof usage.cacheWrite === "number") {
          llmSpan.setAttribute("gen_ai.usage.cache_write_tokens", usage.cacheWrite);
        }
        if (typeof event?.durationMs === "number") {
          llmSpan.setAttribute("openclaw.llm.duration_ms", event.durationMs);
        }

        if (config.captureContent && outputText) {
          llmSpan.setAttribute(ATTR_OBSERVE_ENTITY_OUTPUT, outputText.slice(0, 4096));
        } else if (outputText) {
          llmSpan.setAttribute("openclaw.llm.output_chars", outputText.length);
        }

        if (event?.error) {
          counters.llmErrors.add(1, {
            "gen_ai.response.model": model,
            "openclaw.provider": provider,
          });
          llmSpan.setAttribute("openclaw.llm.error", String(event.error).slice(0, 500));
          llmSpan.setStatus({ code: SpanStatusCode.ERROR, message: String(event.error).slice(0, 200) });
        } else {
          llmSpan.setStatus({ code: SpanStatusCode.OK });
        }

        llmSpan.end();
        sessionCtx.llmSpan = undefined;
      } catch {
        // Never let telemetry errors break the main flow
      }

      return undefined;
    },
    { priority: -80 }
  );

  logger.info("[otel] Registered llm_output hook (via api.on)");

  // ── before_tool_call ────────────────────────────────────────────
  // Starts an explicit tool execution span before result persistence.

  api.on(
    "before_tool_call",
    (event: any, ctx: any) => {
      try {
        const sessionKey = ctx?.sessionKey || event?.sessionKey;
        if (!sessionKey) {
          return undefined;
        }

        const toolName = event?.toolName || "unknown";
        const toolCallId = typeof event?.toolCallId === "string" ? event.toolCallId : undefined;
        const agentId = ctx?.agentId || event?.agentId || "unknown";
        const toolInput = event?.params || event?.input || {};
        const sessionCtx = getOrCreateSessionContext(tracer, sessionKey, logger);
        const parentContext = getParentContext(sessionCtx);

        const span = tracer.startSpan(
          `tool.${toolName}`,
          {
            kind: SpanKind.INTERNAL,
            attributes: {
              [ATTR_OBSERVE_SPAN_KIND]: ObserveSpanKind.TOOL,
              [ATTR_OBSERVE_ENTITY_NAME]: toolName,
              "openclaw.tool.name": toolName,
              "openclaw.tool.call_id": toolCallId || "",
              "openclaw.session.key": sessionKey,
              "openclaw.agent.id": agentId,
            },
          },
          parentContext
        );

        if (config.captureContent && toolInput) {
          const inputStr = typeof toolInput === "string" ? toolInput : JSON.stringify(toolInput);
          if (typeof inputStr === "string") {
            span.setAttribute(ATTR_OBSERVE_ENTITY_INPUT, inputStr.slice(0, 4096));
          }
        }

        const securityEvent = checkToolSecurity(
          toolName,
          toolInput,
          span,
          securityCounters,
          sessionKey,
          agentId
        );
        if (securityEvent) {
          logger.warn?.(`[otel] SECURITY: ${securityEvent.detection} - ${securityEvent.description}`);
        }

        const agentSequence = getHandoffSequence(sessionKey);
        const forkAttrs = registerToolSpan(sessionKey, toolName, span, agentId, agentSequence);
        if (forkAttrs) {
          for (const [k, v] of Object.entries(forkAttrs)) {
            span.setAttribute(k, v);
          }
        }

        const spanKey = getToolSpanKey({ sessionCtx, sessionKey, toolName, toolCallId });
        sessionCtx.toolSpans.set(spanKey, { span, toolName, toolCallId });
        touchSession(sessionKey, parentContext);
      } catch {
        // Never let telemetry errors break the main flow
      }

      return undefined;
    },
    { priority: 80 }
  );

  logger.info("[otel] Registered before_tool_call hook (via api.on)");

  // ── after_tool_call ─────────────────────────────────────────────
  // Completes the explicit tool span with result and error metadata.

  api.on(
    "after_tool_call",
    (event: any, ctx: any) => {
      try {
        const sessionKey = ctx?.sessionKey || event?.sessionKey;
        if (!sessionKey) {
          return undefined;
        }

        const toolName = event?.toolName || "unknown";
        counters.toolCalls.add(1, {
          "tool.name": toolName,
          "session.key": sessionKey,
        });
        if (event?.error) {
          counters.toolErrors.add(1, { "tool.name": toolName });
        }
        if (typeof event?.durationMs === "number") {
          histograms.toolDuration.record(event.durationMs, { "tool.name": toolName });
        }

        completeToolSpan({
          sessionCtx: sessionContextMap.get(sessionKey),
          sessionKey,
          toolName,
          toolCallId: typeof event?.toolCallId === "string" ? event.toolCallId : undefined,
          durationMs: event?.durationMs,
          error: event?.error,
          result: event?.result,
          logger,
        });
      } catch {
        // Never let telemetry errors break the main flow
      }

      return undefined;
    },
    { priority: -90 }
  );

  logger.info("[otel] Registered after_tool_call hook (via api.on)");

  // ── tool_result_persist ──────────────────────────────────────────
  // Creates a fallback tool span when before/after_tool_call are unavailable.
  // SYNCHRONOUS — must not return a Promise.

  api.on(
    "tool_result_persist",
    (event: any, ctx: any) => {
      try {
        const toolName = event?.toolName || "unknown";
        const toolCallId = event?.toolCallId || "";
        const isSynthetic = event?.isSynthetic === true;
        const sessionKey = ctx?.sessionKey || "unknown";
        const agentId = ctx?.agentId || "unknown";

        const sessionCtx = getOrCreateSessionContext(tracer, sessionKey, logger);
        const matchedToolSpan = findToolSpanEntry({
          sessionCtx,
          sessionKey,
          toolName,
          toolCallId: toolCallId || undefined,
        });
        const recentCompletionKey = toolCallId
          ? `session:${sessionKey}:toolcall:${toolCallId}`
          : `session:${sessionKey}:tool:${toolName}`;
        if (matchedToolSpan || wasToolRecentlyCompleted(recentCompletionKey)) {
          return undefined;
        }

        // Tool input is available in event.input for security checks
        const toolInput = event?.input || event?.toolInput || event?.args || {};

        // Record metric
        counters.toolCalls.add(1, {
          "tool.name": toolName,
          "session.key": sessionKey,
        });

        // Get parent context — prefer agent turn span, fall back to root
        const parentContext = sessionCtx?.agentContext || sessionCtx?.rootContext || context.active();

        // Create tool span as child of agent turn
        const span = tracer.startSpan(
          `tool.${toolName}`,
          {
            kind: SpanKind.INTERNAL,
            attributes: {
              [ATTR_OBSERVE_SPAN_KIND]: ObserveSpanKind.TOOL,
              [ATTR_OBSERVE_ENTITY_NAME]: toolName,
              "openclaw.tool.name": toolName,
              "openclaw.tool.call_id": toolCallId,
              "openclaw.tool.is_synthetic": isSynthetic,
              "openclaw.session.key": sessionKey,
              "openclaw.agent.id": agentId,
            },
          },
          parentContext
        );

        // Track session activity
        touchSession(sessionKey, parentContext);

        // Fork detection — register this tool span for parallel detection
        const agentSequence = getHandoffSequence(sessionKey);
        const forkAttrs = registerToolSpan(sessionKey, toolName, span, agentId, agentSequence);
        if (forkAttrs) {
          for (const [k, v] of Object.entries(forkAttrs)) {
            span.setAttribute(k, v);
          }
          logger.info?.(
            `[otel] Tool in fork group: tool=${toolName}, forkId=${forkAttrs["ioa_observe.fork.id"]}, ` +
            `branch=${forkAttrs["ioa_observe.fork.branch_index"]}`
          );
        }

        // Capture tool input if configured
        if (config.captureContent && toolInput) {
          const inputStr = typeof toolInput === "string" ? toolInput : JSON.stringify(toolInput);
          span.setAttribute(ATTR_OBSERVE_ENTITY_INPUT, inputStr.slice(0, 4096));
        }

        // ═══ SECURITY DETECTION 1 & 3: File Access & Dangerous Commands ═══
        const securityEvent = checkToolSecurity(
          toolName,
          toolInput,
          span,
          securityCounters,
          sessionKey,
          agentId
        );
        if (securityEvent) {
          logger.warn?.(`[otel] SECURITY: ${securityEvent.detection} - ${securityEvent.description}`);
          // Add tool input details to span for forensics
          if (toolInput) {
            const inputStr = JSON.stringify(toolInput);
            if (typeof inputStr === "string") {
              span.setAttribute("openclaw.tool.input_preview", inputStr.slice(0, 1000));
            }
          }
        }

        // Inspect the message for result metadata
        const message = event?.message;
        if (message) {
          const contentArray = message?.content;
          if (contentArray && Array.isArray(contentArray)) {
            const textParts = contentArray
              .filter((c: any) => c.type === "text")
              .map((c: any) => String(c.text || ""));
            const totalChars = textParts.reduce((sum: number, t: string) => sum + t.length, 0);
            span.setAttribute("openclaw.tool.result_chars", totalChars);
            span.setAttribute("openclaw.tool.result_parts", contentArray.length);

            // Capture tool output if configured
            if (config.captureContent && textParts.length > 0) {
              span.setAttribute(ATTR_OBSERVE_ENTITY_OUTPUT, textParts.join("").slice(0, 4096));
            }
          }

          if (message?.is_error === true || message?.isError === true) {
            counters.toolErrors.add(1, { "tool.name": toolName });
            span.setStatus({ code: SpanStatusCode.ERROR, message: "Tool execution error" });
          } else if (!securityEvent) {
            // Only set OK status if no security event
            span.setStatus({ code: SpanStatusCode.OK });
          }
        } else if (!securityEvent) {
          span.setStatus({ code: SpanStatusCode.OK });
        }

        span.end();
      } catch {
        // Never let telemetry errors break the main flow
      }

      // Return undefined to keep the tool result unchanged
      return undefined;
    },
    { priority: -100 }
  );

  logger.info("[otel] Registered tool_result_persist hook (via api.on)");

  // ── agent_end ────────────────────────────────────────────────────
  // Ends the agent turn span AND the root request span.
  // Event shape from OpenClaw:
  //   event: { messages, success, error?, durationMs }
  //   ctx:   { agentId, sessionKey, workspaceDir, messageProvider? }
  // Token usage is embedded in the last assistant message's .usage field.

  api.on(
    "agent_end",
    async (event: any, ctx: any) => {
      try {
        const sessionKey = resolveSessionKey(event, ctx);
        const agentId = event?.agentId || ctx?.agentId || "unknown";
        const durationMs = event?.durationMs;
        const success = event?.success !== false;
        const errorMsg = event?.error;

        // Try to get usage from diagnostic events (includes cost!)
        const diagUsage = getPendingUsage(sessionKey);

        // Fallback: Extract token usage from the messages array
        const messages: any[] = event?.messages || [];
        let totalInputTokens = 0;
        let totalOutputTokens = 0;
        let cacheReadTokens = 0;
        let cacheWriteTokens = 0;
        let model = "unknown";
        let costUsd: number | undefined;

        if (diagUsage) {
          // Use diagnostic event data (more accurate, includes cost)
          totalInputTokens = diagUsage.usage.input || 0;
          totalOutputTokens = diagUsage.usage.output || 0;
          cacheReadTokens = diagUsage.usage.cacheRead || 0;
          cacheWriteTokens = diagUsage.usage.cacheWrite || 0;
          model = diagUsage.model || "unknown";
          costUsd = diagUsage.costUsd;
          logger.debug?.(`[otel] agent_end using diagnostic data: cost=$${costUsd?.toFixed(4) || "?"}`);
        } else {
          // Fallback: parse messages manually
          for (const msg of messages) {
            if (msg?.role === "assistant" && msg?.usage) {
              const u = msg.usage;
              // pi-ai stores usage as .input/.output (normalized names)
              if (typeof u.input === "number") totalInputTokens += u.input;
              else if (typeof u.inputTokens === "number") totalInputTokens += u.inputTokens;
              else if (typeof u.input_tokens === "number") totalInputTokens += u.input_tokens;

              if (typeof u.output === "number") totalOutputTokens += u.output;
              else if (typeof u.outputTokens === "number") totalOutputTokens += u.outputTokens;
              else if (typeof u.output_tokens === "number") totalOutputTokens += u.output_tokens;

              if (typeof u.cacheRead === "number") cacheReadTokens += u.cacheRead;
              if (typeof u.cacheWrite === "number") cacheWriteTokens += u.cacheWrite;
            }
            if (msg?.role === "assistant" && msg?.model) {
              model = msg.model;
            }
          }
        }

        const totalTokens = totalInputTokens + totalOutputTokens + cacheReadTokens + cacheWriteTokens;
        logger.debug?.(`[otel] agent_end tokens: input=${totalInputTokens}, output=${totalOutputTokens}, cache_read=${cacheReadTokens}, cache_write=${cacheWriteTokens}, model=${model}`);

        const sessionCtx = sessionContextMap.get(sessionKey);

        if (sessionCtx?.llmSpan) {
          sessionCtx.llmSpan.setAttribute("gen_ai.response.model", model);
          if (errorMsg) {
            sessionCtx.llmSpan.setAttribute("openclaw.llm.error", String(errorMsg).slice(0, 500));
            sessionCtx.llmSpan.setStatus({ code: SpanStatusCode.ERROR, message: String(errorMsg).slice(0, 200) });
          } else {
            sessionCtx.llmSpan.setStatus({ code: SpanStatusCode.OK });
          }
          sessionCtx.llmSpan.end();
          sessionCtx.llmSpan = undefined;
        }

        if (sessionCtx?.toolSpans.size) {
          for (const [spanKey, toolSpanCtx] of sessionCtx.toolSpans) {
            if (errorMsg) {
              toolSpanCtx.span.setAttribute("openclaw.tool.error", String(errorMsg).slice(0, 500));
              toolSpanCtx.span.setStatus({ code: SpanStatusCode.ERROR, message: String(errorMsg).slice(0, 200) });
            } else {
              toolSpanCtx.span.setStatus({ code: SpanStatusCode.OK });
            }
            toolSpanCtx.span.end();
            rememberToolCompletion(spanKey);
            rememberToolCompletion(`session:${sessionKey}:tool:${toolSpanCtx.toolName}`);
          }
          sessionCtx.toolSpans.clear();
        }

        // End the agent turn span
        if (sessionCtx?.agentSpan) {
          const agentSpan = sessionCtx.agentSpan;

          if (typeof durationMs === "number") {
            agentSpan.setAttribute("openclaw.agent.duration_ms", durationMs);
          }

          // Token usage — GenAI semantic convention attributes
          agentSpan.setAttribute("gen_ai.usage.input_tokens", totalInputTokens);
          agentSpan.setAttribute("gen_ai.usage.output_tokens", totalOutputTokens);
          agentSpan.setAttribute("gen_ai.usage.total_tokens", totalTokens);
          agentSpan.setAttribute("gen_ai.response.model", model);
          agentSpan.setAttribute("openclaw.agent.success", success);

          // Cache tokens (custom attributes)
          if (cacheReadTokens > 0) {
            agentSpan.setAttribute("gen_ai.usage.cache_read_tokens", cacheReadTokens);
          }
          if (cacheWriteTokens > 0) {
            agentSpan.setAttribute("gen_ai.usage.cache_write_tokens", cacheWriteTokens);
          }

          // Cost (from diagnostic events) — this is the key addition!
          if (typeof costUsd === "number") {
            agentSpan.setAttribute("openclaw.llm.cost_usd", costUsd);
          }

          // Context window (from diagnostic events)
          if (diagUsage?.context?.limit) {
            agentSpan.setAttribute("openclaw.context.limit", diagUsage.context.limit);
          }
          if (diagUsage?.context?.used) {
            agentSpan.setAttribute("openclaw.context.used", diagUsage.context.used);
          }

          // Record metrics only if we didn't get them from diagnostics
          // (diagnostics module already records metrics on model.usage event)
          if (!diagUsage && (totalInputTokens > 0 || totalOutputTokens > 0)) {
            const metricAttrs = {
              "gen_ai.response.model": model,
              "openclaw.agent.id": agentId,
            };
            counters.tokensPrompt.add(totalInputTokens + cacheReadTokens + cacheWriteTokens, metricAttrs);
            counters.tokensCompletion.add(totalOutputTokens, metricAttrs);
            counters.tokensTotal.add(totalTokens, metricAttrs);
            counters.llmRequests.add(1, metricAttrs);
          }

          // Record duration histogram
          if (typeof durationMs === "number") {
            histograms.agentTurnDuration.record(durationMs, {
              "gen_ai.response.model": model,
              "openclaw.agent.id": agentId,
            });
          }

          // Finalize fork/join detection for this agent turn
          const forkResult = finalizeAgentTurn(sessionKey);
          if (forkResult) {
            agentSpan.setAttribute("ioa_observe.fork.id", forkResult.forkId);
            agentSpan.setAttribute("ioa_observe.fork.branch_count", forkResult.branchCount);
            agentSpan.addEvent("agent.fork_completed", {
              "ioa_observe.fork.id": forkResult.forkId,
              "ioa_observe.fork.branch_count": forkResult.branchCount,
            });
            logger.info?.(
              `[otel] Fork completed: agent=${agentId}, forkId=${forkResult.forkId}, branches=${forkResult.branchCount}`
            );
          }

          // Update handoff state so next agent can link back
          onAgentEnd(sessionKey, agentId, agentSpan);
          logger.info?.(
            `[otel] Agent turn ended: agent=${agentId}, session=${sessionKey}, ` +
            `success=${success}, duration=${durationMs ?? "?"}ms, ` +
            `tokens=${totalTokens}, cost=$${costUsd?.toFixed(4) ?? "?"}`
          );

          if (errorMsg) {
            agentSpan.setAttribute("openclaw.agent.error", String(errorMsg).slice(0, 500));
            agentSpan.setStatus({ code: SpanStatusCode.ERROR, message: String(errorMsg).slice(0, 200) });
          } else {
            agentSpan.setStatus({ code: SpanStatusCode.OK });
          }

          agentSpan.end();
          sessionCtx.agentEnded = true;
        }

        // End the root request span
        if (sessionCtx?.rootSpan && sessionCtx.rootSpan !== sessionCtx.agentSpan) {
          endRootSpan(sessionCtx, errorMsg);
        }

        // Clean up all per-session state
        sessionContextMap.delete(sessionKey);
        activeAgentSpans.delete(sessionKey);
        cleanupHandoff(sessionKey);
        cleanupForkJoin(sessionKey);

        logger.info?.(`[otel] Trace completed for session=${sessionKey}`);
      } catch {
        // Silently ignore
      }
    },
    { priority: -100 }
  );

  logger.info("[otel] Registered agent_end hook (via api.on)");

  // ═══════════════════════════════════════════════════════════════════
  // EVENT-STREAM HOOKS — registered via api.registerHook()
  // ═══════════════════════════════════════════════════════════════════

  // ── Command event hooks ──────────────────────────────────────────

  api.registerHook(
    ["command:new", "command:reset", "command:stop"],
    async (event: any) => {
      try {
        const action = event?.action || "unknown";
        const sessionKey = event?.sessionKey || "unknown";

        // Get parent context if available
        const sessionCtx = sessionContextMap.get(sessionKey);
        const parentContext = sessionCtx?.rootContext || context.active();

        const span = tracer.startSpan(
          `openclaw.command.${action}`,
          {
            kind: SpanKind.INTERNAL,
            attributes: {
              "openclaw.command.action": action,
              "openclaw.command.session_key": sessionKey,
              "openclaw.command.source": event?.context?.commandSource || "unknown",
            },
          },
          parentContext
        );

        if (action === "new" || action === "reset") {
          counters.sessionResets.add(1, {
            "command.source": event?.context?.commandSource || "unknown",
          });
          // End session lifecycle tracking on reset
          endSession(sessionKey);
          logger.info?.(`[otel] Session ended via command:${action}: session=${sessionKey}`);
        }

        span.setStatus({ code: SpanStatusCode.OK });
        span.end();
      } catch {
        // Silently ignore telemetry errors
      }
    },
    {
      name: "otel-command-events",
      description: "Records session command spans via OpenTelemetry",
    }
  );

  logger.info("[otel] Registered command event hooks (via api.registerHook)");

  // ── Gateway startup hook ─────────────────────────────────────────

  api.registerHook(
    "gateway:startup",
    async (event: any) => {
      try {
        const span = tracer.startSpan("openclaw.gateway.startup", {
          kind: SpanKind.INTERNAL,
          attributes: {
            "openclaw.event.type": "gateway",
            "openclaw.event.action": "startup",
          },
        });
        span.setStatus({ code: SpanStatusCode.OK });
        span.end();
      } catch {
        // Silently ignore
      }
    },
    {
      name: "otel-gateway-startup",
      description: "Records gateway startup event via OpenTelemetry",
    }
  );

  logger.info("[otel] Registered gateway:startup hook (via api.registerHook)");

  // ── Periodic cleanup ─────────────────────────────────────────────
  // Safety net: clean up stale session contexts (e.g., if agent_end never fires)
  setInterval(() => {
    const now = Date.now();
    const maxAge = 5 * 60 * 1000; // 5 minutes
    for (const [key, ctx] of sessionContextMap) {
      if (now - ctx.startTime > maxAge) {
        try {
          ctx.llmSpan?.end();
          for (const [spanKey, toolSpanCtx] of ctx.toolSpans) {
            toolSpanCtx.span.end();
            rememberToolCompletion(spanKey);
            rememberToolCompletion(`session:${key}:tool:${toolSpanCtx.toolName}`);
          }
          ctx.toolSpans.clear();
          ctx.agentSpan?.end();
          if (ctx.rootSpan !== ctx.agentSpan) ctx.rootSpan?.end();
        } catch { /* ignore */ }
        sessionContextMap.delete(key);
        cleanupHandoff(key);
        cleanupForkJoin(key);
        logger.debug?.(`[otel] Cleaned up stale trace context for session=${key}`);
      }
    }
    for (const [key, completedAt] of recentToolCompletions) {
      if (now - completedAt > maxAge) {
        recentToolCompletions.delete(key);
      }
    }
  }, 60_000);
}
