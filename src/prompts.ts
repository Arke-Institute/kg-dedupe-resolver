/**
 * Prompts for the AI deduplication judge
 */

import type { EntityInfo } from './types';

// Max characters for the entire prompt payload (200k chars ≈ 50k tokens)
const MAX_PROMPT_CHARS = 200000;

/**
 * System prompt for batch duplicate detection
 */
export const JUDGE_SYSTEM_PROMPT = `You are a deduplication judge for knowledge graph entities.
Given a SOURCE entity and a list of CANDIDATES, identify which candidates represent the SAME real-world thing as the source.

You will receive the full entity manifests as JSON. Consider all available information:
- Labels may differ but refer to the same thing ("Moby Dick" vs "the white whale")
- Types may differ based on extraction context ("creature" vs "character" vs "animal")
- Properties provide semantic context (aliases, dates, attributes, etc.)
- Relationships are critical for disambiguation:
  - Two "John Smith" entities connected to different organizations are likely different people
  - Shared relationships to the same entities suggest they might be the same
- Similar but distinct entities should NOT be marked as duplicates

Output JSON with ONLY the confirmed duplicates:
{
  "duplicates": [
    { "id": "candidate_id", "confidence": 0.0-1.0, "reasoning": "brief explanation" }
  ]
}

Return an empty duplicates array if no candidates are true duplicates.
Be conservative: if uncertain, do NOT include the candidate.`;

/**
 * Strip internal metadata fields that don't help with deduplication
 */
function stripInternalFields(entity: EntityInfo): Record<string, unknown> {
  return {
    id: entity.id,
    type: entity.type,
    properties: entity.properties,
    relationships: entity.relationships?.filter(r =>
      // Keep all relationships except collection membership
      r.predicate !== 'collection'
    ),
  };
}

/**
 * Serialize entity to JSON, truncating if needed
 */
function serializeEntity(entity: EntityInfo, maxChars?: number): string {
  const clean = stripInternalFields(entity);
  let json = JSON.stringify(clean, null, 2);

  if (maxChars && json.length > maxChars) {
    // Truncate and indicate it was cut
    json = json.slice(0, maxChars - 50) + '\n... [truncated]';
  }

  return json;
}

/**
 * Build the user prompt with source entity and candidates as full JSON manifests
 */
export function buildBatchJudgePrompt(source: EntityInfo, candidates: EntityInfo[]): string {
  // Rough budget: ~30% for source, ~70% for candidates
  const sourceJson = serializeEntity(source, MAX_PROMPT_CHARS * 0.3);

  // Divide remaining budget among candidates
  const candidateBudget = Math.floor((MAX_PROMPT_CHARS * 0.6) / candidates.length);
  const candidateJsons = candidates.map((c, i) => {
    const json = serializeEntity(c, candidateBudget);
    return `### Candidate ${i + 1}\n${json}`;
  });

  return `## SOURCE ENTITY
${sourceJson}

## CANDIDATES
${candidateJsons.join('\n\n')}

Which candidates (if any) represent the SAME real-world entity as the source?
Return JSON with the duplicates array containing only confirmed matches.`;
}
