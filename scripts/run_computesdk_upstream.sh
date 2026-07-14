#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
benchmarks_dir="${COMPUTESDK_BENCHMARKS_DIR:-}"

if [[ -z "${benchmarks_dir}" ]]; then
  echo "run_computesdk_upstream: COMPUTESDK_BENCHMARKS_DIR is required" >&2
  exit 1
fi
if [[ -z "${SPORE_API_URL:-}" ]]; then
  echo "run_computesdk_upstream: SPORE_API_URL is required" >&2
  exit 1
fi
if [[ ! -x "${benchmarks_dir}/node_modules/.bin/tsx" ]]; then
  echo "run_computesdk_upstream: install the upstream benchmark dependencies in ${benchmarks_dir}" >&2
  exit 1
fi

cd "${benchmarks_dir}"
exec node_modules/.bin/tsx "${repo_root}/scripts/computesdk_upstream_benchmark.mjs"
