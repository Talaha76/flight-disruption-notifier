/** @type {import('jest').Config} */
module.exports = {
  testEnvironment: 'node',
  transform: { '^.+\\.ts$': ['ts-jest', { tsconfig: 'tsconfig.json', diagnostics: { warnOnly: false } }] },
  testMatch: ['<rootDir>/test/**/*.test.ts'],
  testPathIgnorePatterns: ['/node_modules/', '/cdk.out/'],
  setupFiles: ['<rootDir>/test/setup-env.ts'],
  collectCoverageFrom: ['src/**/*.ts'],
};
