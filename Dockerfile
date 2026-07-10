# syntax=docker/dockerfile:1

ARG GO_VERSION=1.26
ARG SPOREVM_VERSION=0.11.1
ARG SPOREVM_LINUX_ARM64_SHA256=72e81b1e9e1b93e96f3af2d643f822c1a3a1db60a07b0eefe683d09091b8e73e

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-bookworm AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
ARG TARGETOS=linux
ARG TARGETARCH=arm64
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/spore-agent ./cmd/spore-agent \
 && CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/spore-coordinator ./cmd/spore-coordinator

FROM debian:bookworm-slim AS sporevm
ARG TARGETARCH
ARG SPOREVM_VERSION
ARG SPOREVM_LINUX_ARM64_SHA256
ARG SPOREVM_DOWNLOAD_URL
RUN test "$TARGETARCH" = "arm64"
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl tar \
 && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL -o /tmp/spore.tgz "${SPOREVM_DOWNLOAD_URL:-https://github.com/sporevm/sporevm/releases/download/v${SPOREVM_VERSION}/spore_Linux_arm64.tar.gz}" \
 && echo "${SPOREVM_LINUX_ARM64_SHA256}  /tmp/spore.tgz" | sha256sum -c - \
 && tar -xzf /tmp/spore.tgz -C /tmp \
 && if [ -x /tmp/spore_Linux_arm64/bin/spore ]; then install -m 0755 /tmp/spore_Linux_arm64/bin/spore /usr/local/bin/spore; else install -m 0755 /tmp/spore_Linux_arm64/spore /usr/local/bin/spore; fi \
 && mkdir -p /usr/local/share/sporevm \
 && if [ -d /tmp/spore_Linux_arm64/share/sporevm ]; then cp -R /tmp/spore_Linux_arm64/share/sporevm/. /usr/local/share/sporevm/; fi \
 && rm -rf /tmp/spore.tgz /tmp/spore_Linux_arm64

FROM debian:bookworm-slim
ARG TARGETARCH
RUN test "$TARGETARCH" = "arm64"
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=sporevm /usr/local/bin/spore /usr/local/bin/spore
COPY --from=sporevm /usr/local/share/sporevm /usr/local/share/sporevm
COPY --from=build /out/spore-agent /usr/local/bin/spore-agent
COPY --from=build /out/spore-coordinator /usr/local/bin/spore-coordinator
RUN mkdir -p /var/lib/sporevm/bundle-cache /var/lib/sporevm/rootfs-cache /var/lib/sporevm/results /var/lib/sporevm/work
ENTRYPOINT ["/usr/local/bin/spore-agent"]
