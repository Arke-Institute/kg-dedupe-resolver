import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/**/*.test.ts'],
    testTimeout: 180000, // 3 minutes for E2E tests with full manifest processing
    hookTimeout: 90000,
  },
});
