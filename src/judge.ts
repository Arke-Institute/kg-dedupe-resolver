/**
 * AI judge for entity deduplication
 */

import type { KladosLogger } from '@arke-institute/rhiza';
import { callGemini } from './gemini';
import { JUDGE_SYSTEM_PROMPT, buildBatchJudgePrompt } from './prompts';
import type { EntityInfo, BatchJudgeResult, DuplicateMatch } from './types';

/**
 * Judge a batch of candidates against a source entity
 *
 * Makes a single Gemini call with the source and all candidates,
 * returns the list of confirmed duplicates.
 */
export async function judgeBatch(
  source: EntityInfo,
  candidates: EntityInfo[],
  apiKey: string,
  logger: KladosLogger
): Promise<BatchJudgeResult> {
  const response = await callGemini(
    JUDGE_SYSTEM_PROMPT,
    buildBatchJudgePrompt(source, candidates),
    apiKey
  );

  logger.info('Judge response', {
    tokens: response.tokens,
    cost: `$${response.cost_usd.toFixed(5)}`,
    candidates: candidates.length,
  });

  // Parse the response
  let duplicates: DuplicateMatch[] = [];
  try {
    const parsed = JSON.parse(response.content);
    if (Array.isArray(parsed.duplicates)) {
      duplicates = parsed.duplicates.filter(
        (d: unknown): d is DuplicateMatch =>
          typeof d === 'object' &&
          d !== null &&
          typeof (d as DuplicateMatch).id === 'string' &&
          typeof (d as DuplicateMatch).confidence === 'number' &&
          typeof (d as DuplicateMatch).reasoning === 'string'
      );
    }
  } catch (e) {
    logger.error('Failed to parse judge response', {
      error: e instanceof Error ? e.message : String(e),
      responsePreview: response.content.slice(0, 200),
    });
    // Return empty duplicates on parse failure
  }

  return {
    duplicates,
    cost_usd: response.cost_usd,
    tokens: response.tokens,
  };
}
