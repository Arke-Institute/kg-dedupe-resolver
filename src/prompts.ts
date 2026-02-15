/**
 * Prompts for the AI deduplication judge
 */

import type { EntityInfo, EntityRelationship } from './types';

/**
 * System prompt for batch duplicate detection
 */
export const JUDGE_SYSTEM_PROMPT = `You are a deduplication judge for knowledge graph entities.
Given a SOURCE entity and a list of CANDIDATES, identify which candidates represent the SAME real-world thing as the source.

Consider:
- Labels may differ but refer to the same thing ("Moby Dick" vs "the white whale" vs "the Whale")
- Types may differ based on extraction context ("creature" vs "character" vs "animal")
- Properties provide semantic context (aliases, dates, attributes)
- Relationships are critical for disambiguation:
  - Two "John Smith" entities connected to different organizations are likely different people
  - An entity related to "Captain Ahab" is more likely to be Moby Dick related
- Similar but distinct entities should NOT be marked as duplicates (e.g., "Captain Ahab" vs "Captain Bildad" are different captains)

Output JSON with ONLY the confirmed duplicates:
{
  "duplicates": [
    { "id": "candidate_id", "confidence": 0.0-1.0, "reasoning": "brief explanation" }
  ]
}

Return an empty duplicates array if no candidates are true duplicates.
Be conservative: if uncertain, do NOT include the candidate.`;

/**
 * Format properties for display, excluding internal fields
 */
function formatProperties(properties?: Record<string, unknown>): string {
  if (!properties) return 'None';

  // Exclude label and description (shown separately) and internal fields
  const excluded = new Set(['label', 'description', '_id', '_type', '_created', '_updated']);
  const filtered = Object.entries(properties)
    .filter(([key]) => !excluded.has(key) && !key.startsWith('_'))
    .map(([key, value]) => {
      // Truncate long values
      const strValue = typeof value === 'string' ? value : JSON.stringify(value);
      const displayValue = strValue.length > 100 ? strValue.slice(0, 100) + '...' : strValue;
      return `${key}: ${displayValue}`;
    });

  return filtered.length > 0 ? filtered.join(', ') : 'None';
}

/**
 * Format relationships for display
 */
function formatRelationships(relationships?: EntityRelationship[]): string {
  if (!relationships || relationships.length === 0) return 'None';

  // Filter out collection relationships and same_as (we're creating those)
  const meaningful = relationships.filter(r =>
    r.predicate !== 'collection' && r.predicate !== 'same_as'
  );

  if (meaningful.length === 0) return 'None';

  return meaningful
    .slice(0, 10) // Limit to avoid token bloat
    .map(r => {
      const peerInfo = r.peer_label
        ? `${r.peer_label} (${r.peer_type || 'unknown'})`
        : r.peer;
      return `${r.predicate} → ${peerInfo}`;
    })
    .join('; ');
}

/**
 * Build entity display block with all available context
 */
function formatEntity(entity: EntityInfo, prefix: string = ''): string {
  const lines = [
    `${prefix}ID: ${entity.id}`,
    `${prefix}Label: ${entity.label}`,
    `${prefix}Type: ${entity.type}`,
    `${prefix}Description: ${entity.description || 'N/A'}`,
  ];

  // Add properties if available
  if (entity.properties) {
    lines.push(`${prefix}Properties: ${formatProperties(entity.properties)}`);
  }

  // Add relationships if available
  if (entity.relationships && entity.relationships.length > 0) {
    lines.push(`${prefix}Relationships: ${formatRelationships(entity.relationships)}`);
  }

  return lines.join('\n');
}

/**
 * Build the user prompt with source entity and candidates
 */
export function buildBatchJudgePrompt(source: EntityInfo, candidates: EntityInfo[]): string {
  const candidateList = candidates.map((c, i) => {
    return `${i + 1}. ${formatEntity(c, '   ').trimStart()}`;
  }).join('\n\n');

  return `SOURCE ENTITY:
${formatEntity(source, '- ')}

CANDIDATES:
${candidateList}

Which candidates (if any) represent the SAME real-world entity as the source?
Return JSON with the duplicates array containing only confirmed matches.`;
}
