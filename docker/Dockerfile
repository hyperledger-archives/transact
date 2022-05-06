FROM ubuntu:focal

ENV DEBIAN_FRONTEND=noninteractive

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update && \
    apt-get install -y -q --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    unzip \
    libpq-dev \
    libssl-dev \
    pkg-config \
    libzmq3-dev \
    libsqlite3-dev \
    sqlite3 \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

ENV PATH=$PATH:/root/.cargo/bin

# Install Rust
RUN curl https://sh.rustup.rs -sSf > /usr/bin/rustup-init \
 && chmod +x /usr/bin/rustup-init \
 && rustup-init -y \
 && rustup target add wasm32-unknown-unknown \
# Install protoc
 && TARGET_ARCH=$(dpkg --print-architecture) \
 && if [[ $TARGET_ARCH == "arm64" ]]; then \
      PROTOC_ARCH="aarch_64"; \
    elif [[ $TARGET_ARCH == "amd64" ]]; then \
      PROTOC_ARCH="x86_64"; \
    fi \
 && curl -OLsS https://github.com/google/protobuf/releases/download/v3.7.1/protoc-3.7.1-linux-$PROTOC_ARCH.zip \
      && unzip -o protoc-3.7.1-linux-$PROTOC_ARCH.zip -d /usr/local \
      && rm protoc-3.7.1-linux-$PROTOC_ARCH.zip \
# Install just
 && curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to /usr/local/bin

ENV CARGO_INCREMENTAL=0

WORKDIR /project/transact
