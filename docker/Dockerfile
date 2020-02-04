FROM ubuntu:bionic

RUN apt-get update && \
    apt-get install -y \
    curl \
    unzip \
    gcc \
    libssl-dev \
    pkg-config \
    protobuf-compiler \
    libzmq3-dev \
    libsqlite3-dev \
    sqlite3


RUN curl https://sh.rustup.rs -sSf > /usr/bin/rustup-init && \
    chmod +x /usr/bin/rustup-init && \
    rustup-init -y

ENV PATH=$PATH:/root/.cargo/bin \
    CARGO_INCREMENTAL=0

RUN rustup component add clippy && \
    rustup component add rustfmt

WORKDIR /project/transact
