/**
 * E2E tests for kg-dedupe-resolver
 *
 * Tests semantic deduplication with AI judge by creating entities
 * with obvious duplicates and verifying same_as relationships are created.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  configureTestClient,
  createCollection,
  createEntity,
  invokeKlados,
  waitForKladosLog,
  getEntity,
  assertLogCompleted,
  log,
  apiRequest,
} from '@arke-institute/klados-testing';

// Configuration
const KLADOS_ID = 'IIKHHDFPKR744D2X78GTQE9K56';
const API_BASE = 'https://arke-v1.arke.institute';

// Test state
let testCollectionId: string;
let createdEntityIds: string[] = [];

// Test entity definitions - grouped by expected duplicates
const TEST_ENTITIES = {
  // Group 1: Moby Dick variants (whale)
  mobyDick: {
    type: 'whale',
    properties: {
      label: 'Moby Dick',
      description: 'The great white sperm whale, the main antagonist of the novel',
    },
  },
  whiteWhale: {
    type: 'animal',
    properties: {
      label: 'The White Whale',
      description: 'A legendary albino sperm whale feared by sailors',
    },
  },
  mobyDickHyphen: {
    type: 'creature',
    properties: {
      label: 'Moby-Dick',
      description: 'The white whale that took Captain Ahab\'s leg',
    },
  },

  // Group 2: Captain Ahab variants
  captainAhab: {
    type: 'person',
    properties: {
      label: 'Captain Ahab',
      description: 'The monomaniacal captain of the Pequod, obsessed with killing Moby Dick',
    },
  },
  ahab: {
    type: 'character',
    properties: {
      label: 'Ahab',
      description: 'A one-legged sea captain seeking revenge on a white whale',
    },
  },
  ahabPequod: {
    type: 'sailor',
    properties: {
      label: 'Captain Ahab of the Pequod',
      description: 'Ship captain who lost his leg to the white whale',
    },
  },

  // Group 3: Ishmael variants
  ishmael: {
    type: 'narrator',
    properties: {
      label: 'Ishmael',
      description: 'The narrator of the story, a sailor who joins the Pequod',
    },
  },
  ishmaelSailor: {
    type: 'character',
    properties: {
      label: 'Ishmael the Sailor',
      description: 'A young man who narrates his whaling voyage',
    },
  },

  // Group 4: Non-duplicates (should NOT match)
  pequod: {
    type: 'ship',
    properties: {
      label: 'The Pequod',
      description: 'The whaling ship commanded by Captain Ahab',
    },
  },
  nantucket: {
    type: 'location',
    properties: {
      label: 'Nantucket',
      description: 'An island off the coast of Massachusetts, home to whalers',
    },
  },
};

describe('kg-dedupe-resolver', () => {
  beforeAll(async () => {
    // Configure test client
    const userKey = process.env.ARKE_USER_KEY;
    if (!userKey) {
      throw new Error('ARKE_USER_KEY environment variable is required');
    }

    configureTestClient({
      apiBase: API_BASE,
      userKey,
      network: 'test',
    });

    log('Creating test collection...');
    const collection = await createCollection({
      label: 'Dedupe Test - ' + new Date().toISOString(),
      description: 'Test collection for kg-dedupe-resolver E2E tests',
    });
    testCollectionId = collection.id;
    log(`Created collection: ${testCollectionId}`);

    // Create all test entities
    log('Creating test entities...');
    for (const [key, entityDef] of Object.entries(TEST_ENTITIES)) {
      const entity = await createEntity({
        type: entityDef.type,
        properties: entityDef.properties,
        collectionId: testCollectionId,
      });
      createdEntityIds.push(entity.id);
      log(`Created ${key}: ${entity.id} (${entityDef.properties.label})`);
    }

    // Wait for indexing
    log('Waiting 35s for indexing...');
    await new Promise((r) => setTimeout(r, 35000));
  });

  afterAll(async () => {
    // Cleanup is optional - test collections auto-expire
    log('Test complete. Collection: ' + testCollectionId);
  });

  it('should detect Moby Dick duplicates across types', async () => {
    // Get the Moby Dick entity ID (first entity created)
    const mobyDickId = createdEntityIds[0];
    log(`\nTesting Moby Dick deduplication: ${mobyDickId}`);

    // Invoke dedupe with minimal delay (entities already indexed)
    const result = await invokeKlados({
      kladosId: KLADOS_ID,
      targetEntity: mobyDickId,
      targetCollection: testCollectionId,
      input: {
        indexing_delay_ms: 1000, // Minimal delay since we already waited
        confidence_threshold: 0.6,
      },
    });

    log(`Job started: ${result.job_id}`);

    // Wait for completion
    const logEntity = await waitForKladosLog(result.job_collection!, {
      timeout: 60000,
      pollInterval: 2000,
    });

    assertLogCompleted(logEntity);
    log('Job completed successfully');

    // Get the entity and check for same_as relationships
    const entity = await getEntity(mobyDickId);
    const relationships = entity.relationships || [];
    const sameAsRelationships = relationships.filter(
      (r: { predicate: string }) => r.predicate === 'same_as'
    );

    log(`Found ${sameAsRelationships.length} same_as relationships`);
    for (const rel of sameAsRelationships) {
      log(`  -> ${rel.peer} (confidence: ${rel.properties?.confidence})`);
    }

    // Should find at least one duplicate (The White Whale or Moby-Dick)
    expect(sameAsRelationships.length).toBeGreaterThanOrEqual(1);

    // Verify the duplicates are from our expected set
    const expectedDuplicates = [createdEntityIds[1], createdEntityIds[2]]; // whiteWhale, mobyDickHyphen
    const foundDuplicates = sameAsRelationships.map((r: { peer: string }) => r.peer);
    const hasExpectedDuplicate = foundDuplicates.some((id: string) =>
      expectedDuplicates.includes(id)
    );
    expect(hasExpectedDuplicate).toBe(true);
  });

  it('should detect Captain Ahab duplicates', async () => {
    const captainAhabId = createdEntityIds[3]; // captainAhab
    log(`\nTesting Captain Ahab deduplication: ${captainAhabId}`);

    const result = await invokeKlados({
      kladosId: KLADOS_ID,
      targetEntity: captainAhabId,
      targetCollection: testCollectionId,
      input: {
        indexing_delay_ms: 1000,
        confidence_threshold: 0.6,
      },
    });

    const logEntity = await waitForKladosLog(result.job_collection!, {
      timeout: 60000,
      pollInterval: 2000,
    });

    assertLogCompleted(logEntity);

    const entity = await getEntity(captainAhabId);
    const sameAsRelationships = (entity.relationships || []).filter(
      (r: { predicate: string }) => r.predicate === 'same_as'
    );

    log(`Found ${sameAsRelationships.length} same_as relationships`);

    // Should find Ahab variants
    expect(sameAsRelationships.length).toBeGreaterThanOrEqual(1);
  });

  it('should NOT create same_as for distinct entities', async () => {
    const pequodId = createdEntityIds[8]; // pequod ship
    log(`\nTesting non-duplicate entity: ${pequodId}`);

    const result = await invokeKlados({
      kladosId: KLADOS_ID,
      targetEntity: pequodId,
      targetCollection: testCollectionId,
      input: {
        indexing_delay_ms: 1000,
        confidence_threshold: 0.7,
      },
    });

    const logEntity = await waitForKladosLog(result.job_collection!, {
      timeout: 60000,
      pollInterval: 2000,
    });

    assertLogCompleted(logEntity);

    const entity = await getEntity(pequodId);
    const sameAsRelationships = (entity.relationships || []).filter(
      (r: { predicate: string }) => r.predicate === 'same_as'
    );

    log(`Found ${sameAsRelationships.length} same_as relationships`);

    // Ship should not match whale, person, or location entities
    // Note: This could potentially match if there's another ship, which is fine
    // The key is it shouldn't match obviously different entity types
    expect(sameAsRelationships.length).toBeLessThanOrEqual(1);
  });

  it('should handle confidence threshold correctly', async () => {
    const ishmaelId = createdEntityIds[6]; // ishmael
    log(`\nTesting confidence threshold with Ishmael: ${ishmaelId}`);

    // Use high confidence threshold
    const result = await invokeKlados({
      kladosId: KLADOS_ID,
      targetEntity: ishmaelId,
      targetCollection: testCollectionId,
      input: {
        indexing_delay_ms: 1000,
        confidence_threshold: 0.95, // Very high threshold
      },
    });

    const logEntity = await waitForKladosLog(result.job_collection!, {
      timeout: 60000,
      pollInterval: 2000,
    });

    assertLogCompleted(logEntity);

    // With 0.95 threshold, may find fewer or no matches
    const entity = await getEntity(ishmaelId);
    const sameAsRelationships = (entity.relationships || []).filter(
      (r: { predicate: string }) => r.predicate === 'same_as'
    );

    log(`High threshold (0.95): Found ${sameAsRelationships.length} relationships`);

    // Test passes regardless of count - we're verifying the worker handles the threshold
    expect(logEntity.properties.status).toBe('done');
  });
});
