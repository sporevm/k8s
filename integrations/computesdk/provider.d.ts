export interface SporeVMProviderOptions {
  apiUrl: string;
  image?: string;
  memory?: string;
  requestTimeoutMs?: number;
  commandTimeoutMs?: number;
  destroyTimeoutMs?: number;
  fetch?: typeof globalThis.fetch;
}

export interface SporeVMSandboxOptions {
  name?: string;
  image?: string;
  memory?: string;
}

export interface CommandResult {
  exitCode: number;
  stdout: string;
  stderr: string;
}

export interface SporeVMSandbox {
  id: string;
  runCommand(command: string): Promise<CommandResult>;
  destroy(): Promise<void>;
}

export interface SporeVMCompute {
  sandbox: {
    create(options?: SporeVMSandboxOptions): Promise<SporeVMSandbox>;
  };
}

export const DEFAULT_NODE_IMAGE: string;
export function sporevmKubernetes(options: SporeVMProviderOptions): SporeVMCompute;
