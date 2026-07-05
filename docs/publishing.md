# Publishing

Do not store private environment values here. Runtime image tags, chart values,
and cluster-specific overlays for private deployments belong in ops.

Before publishing:

```bash
mise run fleet:test
mise run public:leak-scan
helm lint charts/sporevm-k8s
```

The public repository is:

```bash
git@github.com:sporevm/k8s.git
```

## Runtime Image

Public runtime images should publish to GHCR:

```bash
export SPOREVM_K8S_RUNTIME_IMAGE=ghcr.io/sporevm/k8s-runtime:0.1.0
mise run runtime:image:push
```

Buildkite reads `SPOREVM_RELEASES_GITHUB_TOKEN` from its secret store and publishes
runtime images and Helm charts on matching `v*` tags. Set
`SPOREVM_K8S_GITHUB_USER` only when the token must log in as a user other than
`sporevm`.

## Helm Chart

The chart is published as an OCI artifact under GHCR:

```bash
helm registry login ghcr.io
mise run chart:package
helm push dist/charts/sporevm-k8s-0.1.0.tgz oci://ghcr.io/sporevm/charts
```

That produces:

```text
oci://ghcr.io/sporevm/charts/sporevm-k8s
```

Install from the published chart:

```bash
helm upgrade --install sporevm-k8s oci://ghcr.io/sporevm/charts/sporevm-k8s \
  --version 0.1.0 \
  --namespace sporevm-system \
  --create-namespace
```
