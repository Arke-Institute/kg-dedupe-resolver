/**
 * Core deduplication job logic
 */

import type { ArkeClient } from '@arke-institute/sdk';
import type { KladosRequest, KladosLogger, Output } from '@arke-institute/rhiza';
import { judgeBatch } from './judge';
import type { Env, EntityInfo, EntityRelationship, DedupeProperties } from './types';

/**
 * Context passed to the job processor
 */
export interface ProcessContext {
  request: KladosRequest;
  client: ArkeClient;
  logger: KladosLogger;
  sql: SqlStorage;
  env: Env;
}

/**
 * Result from job processing
 */
export interface ProcessResult {
  outputs?: Output[];
  reschedule?: boolean;
}

/**
 * Sleep for a given number of milliseconds
 */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Process a dedupe job for a single entity
 */
export async function processJob(ctx: ProcessContext): Promise<ProcessResult> {
  const { request, client, logger, env } = ctx;
  const entityId = request.target_entity;
  const collectionId = request.target_collection;

  if (!entityId) {
    throw new Error('No target_entity provided');
  }

  if (!collectionId) {
    throw new Error('No target_collection provided');
  }

  const props = (request.input || {}) as DedupeProperties;
  const includeProperties = props.include_properties !== false; // default true
  const includeRelationships = props.include_relationships !== false; // default true

  // Step 1: Optional indexing delay
  const delay = props.indexing_delay_ms ?? 30000;
  if (delay > 0) {
    const actualDelay = Math.min(delay, 120000);
    logger.info(`Waiting ${actualDelay}ms for indexing`);
    await sleep(actualDelay);
  }

  // Step 2: Fetch source entity
  const { data: entity, error: entityError } = await client.api.GET('/entities/{id}', {
    params: { path: { id: entityId } },
  });

  if (entityError || !entity) {
    throw new Error(`Entity ${entityId} not found: ${entityError?.error || 'unknown error'}`);
  }

  // Build source entity info with full context
  const source: EntityInfo = {
    id: entity.id,
    label: (entity.properties?.label as string) || entity.id,
    type: entity.type,
    description: entity.properties?.description as string | undefined,
  };

  // Include full properties if enabled
  if (includeProperties && entity.properties) {
    source.properties = entity.properties as Record<string, unknown>;
  }

  // Include relationships if enabled
  if (includeRelationships && entity.relationships) {
    source.relationships = (entity.relationships as Array<{
      predicate: string;
      peer: string;
      peer_type?: string;
    }>).map(r => ({
      predicate: r.predicate,
      peer: r.peer,
      peer_type: r.peer_type,
    }));
  }

  logger.info('Processing entity', { label: source.label, type: source.type });

  // Step 3: Semantic search (top 11 to account for self, no type filter)
  // Use expand: "full" to get complete entity data for better disambiguation
  const searchQuery = `${source.label} ${source.description || ''}`.trim();

  const searchResponse = await (client.api.POST as Function)('/search/entities', {
    body: {
      collection_id: collectionId,
      query: searchQuery,
      limit: 11,
      expand: 'full',
    },
  });

  if (searchResponse.error) {
    throw new Error(`Semantic search failed: ${JSON.stringify(searchResponse.error)}`);
  }

  // Step 4: Filter out self, build candidate list (max 10) with full context
  const results = searchResponse.data?.results || [];
  const candidates: EntityInfo[] = results
    .filter((r: { id: string }) => r.id !== entityId)
    .slice(0, 10)
    .map((r: {
      id: string;
      label: string;
      type: string;
      entity?: {
        properties?: Record<string, unknown>;
        relationships?: Array<{ predicate: string; peer: string; peer_type?: string }>;
      };
    }) => {
      const candidate: EntityInfo = {
        id: r.id,
        label: r.label,
        type: r.type,
        description: r.entity?.properties?.description as string | undefined,
      };

      // Include full properties if available and enabled
      if (includeProperties && r.entity?.properties) {
        candidate.properties = r.entity.properties;
      }

      // Include relationships if available and enabled
      if (includeRelationships && r.entity?.relationships) {
        candidate.relationships = r.entity.relationships.map(rel => ({
          predicate: rel.predicate,
          peer: rel.peer,
          peer_type: rel.peer_type,
        }));
      }

      return candidate;
    });

  if (candidates.length === 0) {
    logger.info('No candidates found');
    return { outputs: [entityId] };
  }

  logger.info(`Found ${candidates.length} candidates`);

  // Step 5: Single batch judge call
  const result = await judgeBatch(source, candidates, env.GEMINI_API_KEY, logger);

  // Step 6: Filter by confidence threshold and add same_as relationships
  const confidenceThreshold = props.confidence_threshold ?? 0.7;
  const confirmedDuplicates = result.duplicates.filter(d => d.confidence >= confidenceThreshold);

  if (confirmedDuplicates.length > 0) {
    // Build update with all same_as relationships
    const updates = [{
      entity_id: entityId,
      relationships_add: confirmedDuplicates.map(dup => ({
        predicate: 'same_as',
        peer: dup.id,
        properties: {
          confidence: dup.confidence,
          reasoning: dup.reasoning,
          detected_by: 'kg-dedupe-resolver',
          detected_at: new Date().toISOString(),
        },
      })),
    }];

    // Fire and forget - don't wait for completion
    (client.api.POST as Function)('/updates/additive', { body: { updates } })
      .then(() => console.log('[Dedupe] Relationships added'))
      .catch((err: Error) => logger.error('Failed to add relationships', { error: err.message }));

    logger.info(`Added ${confirmedDuplicates.length} same_as relationships`, {
      peers: confirmedDuplicates.map(d => d.id),
    });
  } else {
    logger.info('No duplicates confirmed above threshold');
  }

  // Step 7: Pass through entity ID
  return { outputs: [entityId] };
}
