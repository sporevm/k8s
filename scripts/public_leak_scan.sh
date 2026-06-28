#!/bin/sh
set -eu

patterns='buildkite\.com/buildkite|github\.com/buildkite/|buildkite/[A-Za-z0-9._-]+|agents:[[:space:]]*queue|queue:[[:space:]]*[A-Za-z0-9._-]+|Tailscale|tailnet|EKS|public\.ecr\.aws|[0-9]+\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com|(^|[^[:alnum:]])[0-9]{12}([^[:alnum:]]|$)|cleanroom|terraform|tfstate|state bucket|state_bucket|ap-southeast-[0-9]|c6g\.metal|gp3|ebs\.csi\.aws\.com|Vanta|ootc|octo'

if rg --hidden --glob '!.git/**' --glob '!.gitignore' --glob '!.dockerignore' --glob '!scripts/public_leak_scan.sh' -n -i -e "$patterns" .; then
  echo "public leak scan failed" >&2
  exit 1
fi

echo "public leak scan ok"
