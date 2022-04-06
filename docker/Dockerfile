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
    protobuf-compiler \
    libzmq3-dev \
    libsqlite3-dev \
    sqlite3 \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*


RUN curl https://sh.rustup.rs -sSf > /usr/bin/rustup-init && \
    chmod +x /usr/bin/rustup-init && \
    rustup-init -y

# Install just
RUN curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to /usr/local/bin

ENV PATH=$PATH:/root/.cargo/bin \
    CARGO_INCREMENTAL=0

RUN rustup component add clippy && \
    rustup component add rustfmt

WORKDIR /project/transact
