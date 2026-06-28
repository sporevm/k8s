#!/usr/bin/env bash
set -euo pipefail

namespace="${SPORE_REGISTRY_NAMESPACE:-sporevm-system}"
service="${SPORE_REGISTRY_SERVICE:-spore-registry}"
tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

cat >"${tmpdir}/openssl.cnf" <<EOF
[req]
prompt = no
distinguished_name = dn
req_extensions = v3_req

[dn]
CN = ${service}.${namespace}.svc.cluster.local

[v3_req]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = ${service}
DNS.2 = ${service}.${namespace}
DNS.3 = ${service}.${namespace}.svc
DNS.4 = ${service}.${namespace}.svc.cluster.local
DNS.5 = localhost
IP.1 = 127.0.0.1
EOF

openssl genrsa -out "${tmpdir}/ca.key" 4096 >/dev/null 2>&1
openssl req -x509 -new -nodes \
  -key "${tmpdir}/ca.key" \
  -sha256 \
  -days 3650 \
  -subj "/CN=sporevm-k8s dev registry CA" \
  -out "${tmpdir}/ca.crt" >/dev/null 2>&1

openssl genrsa -out "${tmpdir}/tls.key" 2048 >/dev/null 2>&1
openssl req -new \
  -key "${tmpdir}/tls.key" \
  -out "${tmpdir}/tls.csr" \
  -config "${tmpdir}/openssl.cnf" >/dev/null 2>&1
openssl x509 -req \
  -in "${tmpdir}/tls.csr" \
  -CA "${tmpdir}/ca.crt" \
  -CAkey "${tmpdir}/ca.key" \
  -CAcreateserial \
  -out "${tmpdir}/tls.crt" \
  -days 825 \
  -sha256 \
  -extensions v3_req \
  -extfile "${tmpdir}/openssl.cnf" >/dev/null 2>&1

kubectl create namespace "${namespace}" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "${namespace}" create secret tls spore-registry-tls \
  --cert "${tmpdir}/tls.crt" \
  --key "${tmpdir}/tls.key" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "${namespace}" create secret generic spore-registry-ca \
  --from-file=ca.crt="${tmpdir}/ca.crt" \
  --dry-run=client -o yaml | kubectl apply -f -

cat <<EOF
created ${namespace}/spore-registry-tls and ${namespace}/spore-registry-ca
cluster image registry: ${service}.${namespace}.svc.cluster.local:5000
EOF
