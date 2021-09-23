# Copyright 2018-2020 Cargill Incorporated
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


crates := '\
    libtransact \
    examples/simple_xo \
    examples/address_generator \
    examples/sabre_command \
    examples/sabre_smallbank \
    examples/sabre_command_executor\
    cli \
    '

features := '\
    --features=experimental \
    --features=stable \
    --features=default \
    --no-default-features \
    '

build:
    #!/usr/bin/env sh
    set -e
    for feature in $(echo {{features}})
    do
        for crate in $(echo {{crates}})
        do
            cmd="cargo build --tests --manifest-path=$crate/Cargo.toml $BUILD_MODE $feature"
            echo "\033[1m$cmd\033[0m"
            $cmd
        done
    done
    cmd="cargo build --tests --manifest-path=libtransact/Cargo.toml --features=sawtooth-compat"
    echo "\033[1m$cmd\033[0m"
    $cmd
    cmd="cargo build --tests --manifest-path=libtransact/Cargo.toml --features=experimental,state-merkle-redis-db-tests,state-merkle-sql-postgres-tests"
    echo "\033[1m$cmd\033[0m"
    $cmd
    echo "\n\033[92mBuild Success\033[0m\n"

ci:
    just ci-lint
    just ci-test
    just ci-docs
    just ci-debs

ci-debs:
    #!/usr/bin/env sh
    set -e
    REPO_VERSION=$(VERSION=AUTO_STRICT ./bin/get_version) \
    docker-compose -f docker-compose-installed.yaml build
    docker-compose -f docker/compose/copy-debs.yaml up

ci-doc:
    #!/usr/bin/env sh
    set -e
    docker-compose -f docker/compose/docker-compose.yaml build
    docker-compose -f docker/compose/docker-compose.yaml run --rm transact \
      /bin/bash -c "just doc" --abort-on-container-exit transact

ci-lint:
    #!/usr/bin/env sh
    set -e
    docker-compose -f docker/compose/docker-compose.yaml build
    docker-compose -f docker/compose/docker-compose.yaml run --rm transact \
      /bin/bash -c "just lint" --abort-on-container-exit transact

ci-test:
    #!/usr/bin/env sh
    set -e

    trap "docker-compose -f docker/compose/docker-compose.yaml down" EXIT

    docker-compose -f docker/compose/docker-compose.yaml build
    docker-compose -f docker/compose/docker-compose.yaml run --rm transact \
      /bin/bash -c "just test" --abort-on-container-exit transact

    docker-compose -f docker/compose/docker-compose.yaml up --detach redis

    docker-compose -f docker/compose/docker-compose.yaml up --detach postgres-db

    docker-compose -f docker/compose/docker-compose.yaml run --rm transact \
       /bin/bash -c \
       "cargo test --manifest-path /project/transact/libtransact/Cargo.toml \
          --features sawtooth-compat && \
        cargo test --manifest-path /project/transact/libtransact/Cargo.toml \
          --features experimental,state-merkle-redis-db-tests,state-merkle-sql-postgres-tests && \
        (cd examples/sabre_smallbank && cargo test)"

clean:
    cargo clean

copy-env:
    #!/usr/bin/env sh
    set -e
    find . -name .env | xargs -I '{}' sh -c "echo 'Copying to {}'; rsync .env {}"

doc:
    #!/usr/bin/env sh
    set -e
    cargo doc \
        --manifest-path libtransact/Cargo.toml \
        --features stable,experimental \
        --no-deps

lint:
    #!/usr/bin/env sh
    set -e
    echo "\033[1mcargo fmt -- --check\033[0m"
    cargo fmt -- --check
    for feature in $(echo {{features}})
    do
        for crate in $(echo {{crates}})
        do
            cmd="cargo clippy --manifest-path=$crate/Cargo.toml $feature -- -D warnings"
            echo "\033[1m$cmd\033[0m"
            $cmd
        done
    done
    echo "\n\033[92mLint Success\033[0m\n"

test: build
    #!/usr/bin/env sh
    set -e
    for feature in $(echo {{features}})
    do
        for crate in $(echo {{crates}})
        do
            cmd="cargo test --manifest-path=$crate/Cargo.toml $TEST_MODE $feature"
            echo "\033[1m$cmd\033[0m"
            $cmd
        done
    done
    echo "\n\033[92mTest Success\033[0m\n"
