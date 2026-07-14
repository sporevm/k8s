import path from 'node:path';
import { pathToFileURL } from 'node:url';

import { sporevmKubernetes } from '../integrations/computesdk/provider.mjs';

const benchmarksDir = requiredEnv('COMPUTESDK_BENCHMARKS_DIR');
const apiUrl = requiredEnv('SPORE_API_URL');
const iterations = Number.parseInt(process.env.SPORE_COMPUTESDK_ITERATIONS ?? '3', 10);
if (!Number.isInteger(iterations) || iterations < 1) throw new Error('SPORE_COMPUTESDK_ITERATIONS must be >= 1');

const benchmarkModule = pathToFileURL(path.join(benchmarksDir, 'src/sandbox/benchmark.ts')).href;
const { runBenchmark } = await import(benchmarkModule);
const result = await runBenchmark({
  name: 'sporevm-k8s',
  iterations,
  requiredEnvVars: [],
  createCompute: () => sporevmKubernetes({
    apiUrl,
    image: process.env.SPORE_COMPUTESDK_IMAGE,
    memory: process.env.SPORE_COMPUTESDK_MEMORY,
  }),
});

console.log(`SPOREVM_COMPUTESDK_RESULT=${JSON.stringify(result)}`);
const failures = result.iterations.filter(iteration => iteration.error);
if (result.skipped || failures.length > 0 || result.iterations.length !== iterations) process.exitCode = 1;

function requiredEnv(name) {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}
