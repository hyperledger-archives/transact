# Copyright 2018-2021 Cargill Incorporated
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
    sdks/rust \
    cli \
    tp \
    example/intkey_multiply/processor \
    example/intkey_multiply/cli \
    integration \
    '

crates_wasm := '\
    sdks/rust \
    example/intkey_multiply/processor \
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
        for crate in $(echo {{crates_wasm}})
        do
            cmd="cargo build --target wasm32-unknown-unknown --tests --manifest-path=$crate/Cargo.toml $BUILD_MODE $feature"
            echo "\033[1m$cmd\033[0m"
            $cmd
        done
    done
    echo "\n\033[92mBuild Success\033[0m\n"

build-docs:
    #!/usr/bin/env sh
    set -e
    docker-compose -f docs/docker-compose.yaml up --exit-code-from sabre-docs

ci:
    just ci-lint
    just ci-test
    just ci-integration-test
    just ci-build-docs
    just ci-docker-build

ci-build-docs: build-docs

ci-docker-build: docker-build

ci-lint:
    #!/usr/bin/env sh
    set -e
    docker build . -f docker/lint -t lint-sabre
    docker run --rm -v $(pwd):/project/sawtooth-sabre lint-sabre

ci-integration-test: integration-test

ci-test:
    #!/usr/bin/env sh
    set -e
    docker-compose -f docker/unit-test.yaml up --build --exit-code-from unit-test-sabre

clean:
    #!/usr/bin/env sh
    set -e
    for crate in $(echo {{crates}})
    do
        cmd="cargo clean --manifest-path=$crate/Cargo.toml"
        echo "\033[1m$cmd\033[0m"
        $cmd
    done

copy-env:
    #!/usr/bin/env sh
    set -e
    find . -name .env | xargs -I '{}' sh -c "echo 'Copying to {}'; rsync .env {}"


docker-build:
    #!/usr/bin/env sh
    set -e
    export VERSION=AUTO_STRICT
    export REPO_VERSION=$(./bin/get_version)
    docker-compose -f docker-compose-installed.yaml build

lint: lint-ignore
    #!/usr/bin/env sh
    set -e
    for crate in $(echo {{crates}})
    do
        cmd="cargo fmt --manifest-path=$crate/Cargo.toml -- --check"
        echo "\033[1m$cmd\033[0m"
        $cmd
    done
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

lint-ignore:
    #!/usr/bin/env sh
    set -e
    diff -u .dockerignore .gitignore
    echo "\n\033[92mLint Ignore Files Success\033[0m\n"

integration-test:
    #!/usr/bin/env sh
    docker-compose \
      -f docker-compose.yaml \
      -f integration/sabre_test.yaml \
      up \
      --build \
      --abort-on-container-exit \
      --exit-code-from test_sabre

test:
    #!/usr/bin/env sh
    set -e
    for feature in $(echo {{features}})
    do
        for crate in $(echo {{crates}})
        do
            if [ $crate != "integration" ]; then
                cmd="cargo test --manifest-path=$crate/Cargo.toml $TEST_MODE $feature"
                echo "\033[1m$cmd\033[0m"
                $cmd
            fi
        done
    done
    echo "\n\033[92mTest Success\033[0m\n"
