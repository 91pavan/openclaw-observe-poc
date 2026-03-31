/**
 * OpenClaw OTel GenAI Preload Script
 *
 * Usage:
 *   NODE_OPTIONS="--import /path/to/preload.mjs" openclaw gateway start
 *
 * CRITICAL: In Node 22+, ESM loader hooks must be registered via
 * register() from node:module. Simply importing hook.mjs doesn't work.
 */

import { register } from 'node:module';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';

// Step 1: Register IITM as an ESM module loader hook
// This MUST happen before any instrumented modules are imported.
const require = createRequire(import.meta.url);
const iitmHookPath = require.resolve('import-in-the-middle/hook.mjs');
register(pathToFileURL(iitmHookPath).href, { parentURL: import.meta.url });

// Step 2: Set up the OTel SDK with GenAI instrumentations
const { NodeSDK } = await import("@opentelemetry/sdk-node");
const { OTLPTraceExporter } = await import("@opentelemetry/exporter-trace-otlp-proto");
const { BatchSpanProcessor } = await import("@opentelemetry/sdk-trace-node");
const { resourceFromAttributes } = await import("@opentelemetry/resources");
const { AnthropicInstrumentation } = await import("@traceloop/instrumentation-anthropic");
const { OpenAIInstrumentation } = await import("@traceloop/instrumentation-openai");

const OTLP_ENDPOINT = process.env.OTEL_EXPORTER_OTLP_ENDPOINT || "http://localhost:4318";
const SERVICE_NAME = process.env.OTEL_SERVICE_NAME || "openclaw-gateway";

const resource = resourceFromAttributes({
  "service.name": SERVICE_NAME,
  "service.version": "0.1.0",
  "telemetry.sdk.name": "openclaw-otel-preload",
});

const traceExporter = new OTLPTraceExporter({
  url: `${OTLP_ENDPOINT}/v1/traces`,
});

const sdk = new NodeSDK({
  resource,
  spanProcessors: [new BatchSpanProcessor(traceExporter)],
  instrumentations: [
    new AnthropicInstrumentation({ traceContent: false }),
    new OpenAIInstrumentation({ traceContent: false }),
  ],
});

sdk.start();

// Signal to the plugin that preload is active
globalThis.__OPENCLAW_OTEL_PRELOAD_ACTIVE = true;

process.on("SIGTERM", () => sdk.shutdown());
process.on("SIGINT", () => sdk.shutdown());

console.log(`[otel-preload] GenAI instrumentation active (endpoint=${OTLP_ENDPOINT}, IITM loader registered)`);
