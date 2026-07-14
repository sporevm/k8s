import assert from 'node:assert/strict';
import { after, before, beforeEach, test } from 'node:test';
import { createServer } from 'node:http';

import { sporevmKubernetes } from './provider.mjs';

const requests = [];
let server;
let apiUrl;

before(async () => {
  server = createServer(async (request, response) => {
    const body = await readBody(request);
    requests.push({ method: request.method, path: request.url, body });
    response.setHeader('content-type', 'application/json');
    if (request.method === 'POST' && request.url === '/sandboxes') {
      if (body.name === 'fail-create') {
        response.statusCode = 500;
        response.end(JSON.stringify({ error: 'restore failed' }));
        return;
      }
      response.end(JSON.stringify({ name: body.name }));
      return;
    }
    if (request.method === 'POST' && request.url.endsWith('/exec')) {
      response.end(JSON.stringify([
        { event: 'stdout', data_base64: Buffer.from('v22.0.0\n').toString('base64') },
        { event: 'stderr', data_base64: Buffer.from('warning\n').toString('base64') },
        { event: 'exit', exit_code: 0 },
      ]));
      return;
    }
    if (request.method === 'DELETE' && request.url.startsWith('/sandboxes/')) {
      response.end(JSON.stringify({ name: decodeURIComponent(request.url.split('/').at(-1)) }));
      return;
    }
    response.statusCode = 404;
    response.end(JSON.stringify({ error: 'not found' }));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  apiUrl = `http://127.0.0.1:${server.address().port}`;
});

after(async () => {
  await new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
});

beforeEach(() => requests.splice(0));

test('implements the ComputeSDK sandbox lifecycle', async () => {
  const compute = sporevmKubernetes({
    apiUrl,
    image: 'example.com/node@sha256:abc',
    memory: '512mb',
  });
  const sandbox = await compute.sandbox.create({ name: 'benchmark sandbox' });
  const result = await sandbox.runCommand('node -v');
  await sandbox.destroy();
  await sandbox.destroy();

  assert.deepEqual(result, { exitCode: 0, stdout: 'v22.0.0\n', stderr: 'warning\n' });
  assert.deepEqual(requests, [
    {
      method: 'POST',
      path: '/sandboxes',
      body: {
        name: 'benchmark sandbox',
        image: 'example.com/node@sha256:abc',
        memory: '512mb',
      },
    },
    {
      method: 'POST',
      path: '/sandboxes/benchmark%20sandbox/exec',
      body: { command: ['/bin/sh', '-lc', 'node -v'] },
    },
    { method: 'DELETE', path: '/sandboxes/benchmark%20sandbox', body: undefined },
  ]);
});

test('attempts cleanup when sandbox creation fails', async () => {
  const compute = sporevmKubernetes({ apiUrl });

  await assert.rejects(compute.sandbox.create({ name: 'fail-create' }), /restore failed/);

  assert.deepEqual(requests.map(request => [request.method, request.path]), [
    ['POST', '/sandboxes'],
    ['DELETE', '/sandboxes/fail-create'],
  ]);
});

async function readBody(request) {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  return chunks.length > 0 ? JSON.parse(Buffer.concat(chunks).toString()) : undefined;
}
