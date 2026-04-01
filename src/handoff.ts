/**
 * Agent Handoff Tracking — tracks agent execution chains via OTel Span Links.
 *
 * Ported from agntcy/observe SDK concepts:
 *   - Each agent span is annotated with its position in the execution chain
 *   - Span links with link.type = "agent_handoff" connect sequential agents
 *   - Cross-process propagation headers carry handoff context
 *
 * When agent B runs after agent A in the same session, agent B's span gets:
 *   - ioa_observe.agent.sequence = 2
 *   - ioa_observe.agent.previous = "agent_A_name"
 *   - A span link pointing to agent A's span with link.type = "agent_handoff"
 */

import type { Span, SpanContext, Link } from "@opentelemetry/api";

// ── Logger ─────────────────────────────────────────────────────────

let loggerRef: any = null;

/** Set module-level logger (called once during plugin init) */
export function setHandoffLogger(logger: any): void {
  loggerRef = logger;
}

// ── Per-Session Handoff State ──────────────────────────────────────

interface HandoffState {
  /** Last agent's span context (for creating span links) */
  lastAgentSpanContext: SpanContext;
  /** Last agent's name/ID */
  lastAgentName: string;
  /** Current sequence number (1-based) */
  sequence: number;
}

/** Map of sessionKey → handoff state for tracking agent chains */
const handoffMap = new Map<string, HandoffState>();

/**
 * Called when a new agent span starts. Returns span links and attributes
 * to annotate the agent span with handoff metadata.
 */
export function onAgentStart(
  sessionKey: string,
  agentId: string,
  agentSpan: Span
): { links: Link[]; attributes: Record<string, string | number> } {
  const state = handoffMap.get(sessionKey);
  const links: Link[] = [];
  const attributes: Record<string, string | number> = {};

  if (state) {
    // There was a previous agent in this session — create a handoff link
    const sequence = state.sequence + 1;

    links.push({
      context: state.lastAgentSpanContext,
      attributes: {
        "link.type": "agent_handoff",
        "ioa_observe.agent.previous": state.lastAgentName,
        "ioa_observe.agent.previous_sequence": state.sequence,
      },
    });

    attributes["ioa_observe.agent.sequence"] = sequence;
    attributes["ioa_observe.agent.previous"] = state.lastAgentName;

    // Update state for next agent in chain
    handoffMap.set(sessionKey, {
      lastAgentSpanContext: agentSpan.spanContext(),
      lastAgentName: agentId,
      sequence,
    });

    loggerRef?.info?.(
      `[otel:handoff] Agent handoff detected: session=${sessionKey}, ` +
      `previous=${state.lastAgentName} (seq=${state.sequence}) → current=${agentId} (seq=${sequence})`
    );
    loggerRef?.debug?.(
      `[otel:handoff]   spanLink=traceId:${state.lastAgentSpanContext.traceId}/spanId:${state.lastAgentSpanContext.spanId}`
    );
  } else {
    // First agent in this session
    attributes["ioa_observe.agent.sequence"] = 1;

    handoffMap.set(sessionKey, {
      lastAgentSpanContext: agentSpan.spanContext(),
      lastAgentName: agentId,
      sequence: 1,
    });

    loggerRef?.info?.(
      `[otel:handoff] First agent in chain: session=${sessionKey}, agent=${agentId}, seq=1`
    );
  }

  return { links, attributes };
}

/**
 * Called when an agent span ends. Updates the stored span context
 * so subsequent agents can link back to this one.
 */
export function onAgentEnd(sessionKey: string, agentId: string, agentSpan: Span): void {
  const state = handoffMap.get(sessionKey);
  if (state) {
    state.lastAgentSpanContext = agentSpan.spanContext();
    state.lastAgentName = agentId;
    loggerRef?.debug?.(
      `[otel:handoff] Agent ended, updated handoff state: session=${sessionKey}, ` +
      `agent=${agentId}, seq=${state.sequence}, spanId=${agentSpan.spanContext().spanId}`
    );
  }
}

/**
 * Clean up handoff state for a session.
 * Called when the root request span ends.
 */
export function cleanupHandoff(sessionKey: string): void {
  const had = handoffMap.has(sessionKey);
  handoffMap.delete(sessionKey);
  if (had) {
    loggerRef?.debug?.(`[otel:handoff] Cleaned up handoff state for session=${sessionKey}`);
  }
}

/**
 * Get the current handoff sequence for a session.
 */
export function getHandoffSequence(sessionKey: string): number {
  return handoffMap.get(sessionKey)?.sequence ?? 0;
}
