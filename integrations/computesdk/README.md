# ComputeSDK adapter

This package maps the ComputeSDK sandbox lifecycle onto the resident SporeVM
Kubernetes API:

- `sandbox.create()` calls `POST /sandboxes` and waits for exec-ready restore.
- `sandbox.runCommand()` calls `POST /sandboxes/{name}/exec` and decodes the
  SporeVM event stream into `exitCode`, `stdout`, and `stderr`.
- `sandbox.destroy()` calls `DELETE /sandboxes/{name}` and releases the agent
  slot.

The default workload is a digest-pinned Node image. Override it only with
another immutable image reference.

```js
import { sporevmKubernetes } from '@sporevm/computesdk-k8s';

const compute = sporevmKubernetes({
  apiUrl: 'http://spore-api:8081',
});

const sandbox = await compute.sandbox.create();
try {
  console.log(await sandbox.runCommand('node -v'));
} finally {
  await sandbox.destroy();
}
```

`sandbox.create()` plus the first `runCommand()` matches the upstream
ComputeSDK sequential TTI boundary. It measures named sandbox restore and first
exec, while `POST /runs` remains the separate one-shot fresh-child API.
