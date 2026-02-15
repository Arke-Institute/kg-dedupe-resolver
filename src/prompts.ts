/**
 * Prompts for the AI deduplication judge
 */

import type { EntityInfo } from './types';

/**
 * System prompt for batch duplicate detection
 */
export const JUDGE_SYSTEM_PROMPT = `You are a deduplication judge for knowledge graph entities.
Given a SOURCE entity and a list of CANDIDATES, identify which candidates represent the SAME real-world thing as the source.

Consider:
- Labels may differ but refer to the same thing ("Moby Dick" vs "the white whale" vs "the Whale")
- Types may differ based on extraction context ("creature" vs "character" vs "animal")
- Descriptions and properties provide additional semantic context
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
 * Build the user prompt with source entity and candidates
 */
export function buildBatchJudgePrompt(source: EntityInfo, candidates: EntityInfo[]): string {
  const candidateList = candidates.map((c, i) =>
    `${i + 1}. ID: ${c.id}
   Label: ${c.label}
   Type: ${c.type}
   Description: ${c.description || 'N/A'}`
  ).join('\n\n');

  return `SOURCE ENTITY:
- ID: ${source.id}
- Label: ${source.label}
- Type: ${source.type}
- Description: ${source.description || 'N/A'}

CANDIDATES:
${candidateList}

Which candidates (if any) represent the SAME real-world entity as the source?
Return JSON with the duplicates array containing only confirmed matches.`;
}
