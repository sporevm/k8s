import { randomUUID } from 'node:crypto';

export const DEFAULT_NODE_IMAGE = 'docker.io/library/node@sha256:6db9be2ebb4bafb687a078ef5ba1b1dd256e8004d246a31fd210b6b848ab6be2';

export function sporevmKubernetes(options) {
  if (!options?.apiUrl) throw new Error('sporevmKubernetes requires apiUrl');
  const client = new APIClient(options.apiUrl, options.fetch ?? globalThis.fetch, options.requestTimeoutMs ?? 110_000);
  const defaults = {
    image: options.image ?? DEFAULT_NODE_IMAGE,
    memory: options.memory ?? '1024mb',
    commandTimeoutMs: options.commandTimeoutMs ?? 25_000,
    destroyTimeoutMs: options.destroyTimeoutMs ?? 10_000,
  };

  return {
    sandbox: {
      async create(sandboxOptions = {}) {
        const name = sandboxOptions.name ?? `computesdk-${randomUUID()}`;
        try {
          await client.request('POST', '/sandboxes', {
            name,
            image: sandboxOptions.image ?? defaults.image,
            memory: sandboxOptions.memory ?? defaults.memory,
          });
        } catch (error) {
          await client.request('DELETE', `/sandboxes/${encodeURIComponent(name)}`, undefined, defaults.destroyTimeoutMs).catch(() => {});
          throw error;
        }
        return new SporeVMSandbox(client, name, defaults.commandTimeoutMs, defaults.destroyTimeoutMs);
      },
    },
  };
}

class SporeVMSandbox {
  constructor(client, name, commandTimeoutMs, destroyTimeoutMs) {
    this.client = client;
    this.id = name;
    this.commandTimeoutMs = commandTimeoutMs;
    this.destroyTimeoutMs = destroyTimeoutMs;
    this.destroyed = false;
  }

  async runCommand(command) {
    if (this.destroyed) throw new Error(`sandbox ${this.id} has been destroyed`);
    if (typeof command !== 'string' || command.length === 0) throw new Error('runCommand requires a command string');
    const events = await this.client.request(
      'POST',
      `/sandboxes/${encodeURIComponent(this.id)}/exec`,
      { command: ['/bin/sh', '-lc', command] },
      this.commandTimeoutMs,
    );
    if (!Array.isArray(events)) throw new Error('sandbox exec response did not contain events');

    let stdout = '';
    let stderr = '';
    let terminal;
    for (const event of events) {
      if (event?.event === 'stdout' && event.data_base64) stdout += Buffer.from(event.data_base64, 'base64').toString();
      if (event?.event === 'stderr' && event.data_base64) stderr += Buffer.from(event.data_base64, 'base64').toString();
      if (event?.event === 'exit' || event?.event === 'failure') terminal = event;
    }
    if (!terminal) throw new Error('sandbox exec response had no terminal event');
    if (terminal.event === 'failure') {
      stderr += terminal.error?.message ?? 'SporeVM command failed';
    }
    return {
      exitCode: terminal.event === 'exit' && Number.isInteger(terminal.exit_code) ? terminal.exit_code : 1,
      stdout,
      stderr,
    };
  }

  async destroy() {
    if (this.destroyed) return;
    await this.client.request('DELETE', `/sandboxes/${encodeURIComponent(this.id)}`, undefined, this.destroyTimeoutMs);
    this.destroyed = true;
  }
}

class APIClient {
  constructor(apiUrl, fetchImpl, timeoutMs) {
    if (typeof fetchImpl !== 'function') throw new Error('sporevmKubernetes requires a fetch implementation');
    this.apiUrl = apiUrl.replace(/\/$/, '');
    this.fetch = fetchImpl;
    this.timeoutMs = timeoutMs;
  }

  async request(method, path, payload, timeoutMs = this.timeoutMs) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await this.fetch(this.apiUrl + path, {
        method,
        headers: payload === undefined ? undefined : { 'content-type': 'application/json' },
        body: payload === undefined ? undefined : JSON.stringify(payload),
        signal: controller.signal,
      });
      const body = await response.text();
      if (!response.ok) throw new Error(`${method} ${path}: HTTP ${response.status}: ${body}`);
      return body ? JSON.parse(body) : null;
    } finally {
      clearTimeout(timer);
    }
  }
}
