# syntax=docker/dockerfile:1

ARG GO_VERSION=1.26
ARG SPOREVM_VERSION=0.18.6
ARG SPOREVM_LINUX_ARM64_SHA256=f79b9aacd8656b19bf4757ff2abed86f397f47b3cbe6b5677166d5c843b6bd6a

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
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
ARG AWS_CLI_VERSION=2.36.7
ARG AWS_CLI_ARM64_SHA256=85826b67912b44bb45d1e46c6e66f383c14405ee0b2f4686f73bdf949c93bd61
RUN test "$TARGETARCH" = "arm64"
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl tar unzip \
 && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL -o /tmp/awscli.zip "https://awscli.amazonaws.com/awscli-exe-linux-aarch64-${AWS_CLI_VERSION}.zip" \
 && echo "${AWS_CLI_ARM64_SHA256}  /tmp/awscli.zip" | sha256sum -c - \
 && unzip -q /tmp/awscli.zip -d /tmp \
 && /tmp/aws/install \
 && rm -rf /tmp/aws /tmp/awscli.zip
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
COPY --from=sporevm /usr/local/aws-cli /usr/local/aws-cli
RUN ln -s /usr/local/aws-cli/v2/current/bin/aws /usr/local/bin/aws
COPY --from=build /out/spore-agent /usr/local/bin/spore-agent
COPY --from=build /out/spore-coordinator /usr/local/bin/spore-coordinator
RUN mkdir -p /var/lib/sporevm/rootfs-cache /var/lib/sporevm/results /var/lib/sporevm/work
ENTRYPOINT ["/usr/local/bin/spore-agent"]
