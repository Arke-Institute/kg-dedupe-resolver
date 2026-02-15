import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/**/*.test.ts'],
    testTimeout: 120000, // 2 minutes for E2E tests with indexing delays
    hookTimeout: 60000,
  },
});
