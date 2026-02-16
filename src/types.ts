/**
 * Type definitions for KG Dedupe Resolver
 */

/**
 * Cloudflare Worker environment bindings
 */
export interface Env {
  /** Klados agent ID (registered in Arke) */
  AGENT_ID: string;

  /** Agent version for logging */
  AGENT_VERSION: string;

  /** Arke agent API key (secret) */
  ARKE_AGENT_KEY: string;

  /** Gemini API key for AI judge calls (secret) */
  GEMINI_API_KEY: string;

  /** Verification token for endpoint verification */
  VERIFICATION_TOKEN?: string;

  /** Agent ID for verification (used before AGENT_ID is configured) */
  ARKE_VERIFY_AGENT_ID?: string;

  /** Durable Object binding for job processing */
  KLADOS_JOB: DurableObjectNamespace;
}

/**
 * Raw relationship from entity manifest
 */
export interface RawRelationship {
  predicate: string;
  peer: string;
  peer_type?: string;
  properties?: Record<string, unknown>;
}

/**
 * Entity information for deduplication - passes through raw manifest data
 */
export interface EntityInfo {
  id: string;
  type: string;
  /** Full properties object from entity manifest */
  properties: Record<string, unknown>;
  /** Full relationships array from entity manifest */
  relationships?: RawRelationship[];
}

/**
 * A confirmed duplicate match from the AI judge
 */
export interface DuplicateMatch {
  id: string;
  confidence: number;
  reasoning: string;
}

/**
 * Result from the batch judge call
 */
export interface BatchJudgeResult {
  duplicates: DuplicateMatch[];
  cost_usd: number;
  tokens: number;
}

/**
 * Gemini API response structure
 */
export interface GeminiResponse {
  content: string;
  tokens: number;
  prompt_tokens: number;
  completion_tokens: number;
  cost_usd: number;
}

/**
 * Properties that may be passed in the request
 */
export interface DedupeProperties {
  /** Optional delay in ms to wait for indexing (default: 30000, max: 120000) */
  indexing_delay_ms?: number;
  /** Confidence threshold for accepting duplicates (default: 0.7) */
  confidence_threshold?: number;
}
