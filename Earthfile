VERSION 0.7
FROM ubuntu:22.04

RUN apt-get update && apt-get install --yes sudo

WORKDIR /dbsp
ENV RUSTUP_HOME=$HOME/.rustup
ENV CARGO_HOME=$HOME/.cargo
# Adds python and rust binaries to thep path
ENV PATH=$HOME/.cargo/bin:$HOME/.local/bin:$PATH
ENV RUST_VERSION=1.73.0
ENV RUST_BUILD_MODE='' # set to --release for release builds

install-deps:
    RUN apt-get update
    RUN apt-get install --yes build-essential curl libssl-dev build-essential pkg-config \
                              cmake git gcc clang libclang-dev python3-pip python3-plumbum \
                              hub numactl openjdk-19-jre-headless maven netcat jq \
                              docker.io libenchant-2-2 graphviz locales
    # Set UTF-8 locale. Needed for the Rust compiler to handle Unicode column names.
    RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
        locale-gen
    ENV LC_ALL en_US.UTF-8
    ENV LANG en_US.UTF-8
    # NodeJS install
    RUN sudo apt-get install -y ca-certificates curl gnupg
    RUN sudo mkdir -p /etc/apt/keyrings
    RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | sudo gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
    ENV NODE_MAJOR=20
    RUN echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" | sudo tee /etc/apt/sources.list.d/nodesource.list
    RUN sudo apt-get update
    RUN sudo apt-get install nodejs -y
    RUN npm install --global yarn
    RUN npm install --global openapi-typescript-codegen

install-rust:
    FROM +install-deps
    RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- \
        -y \
        --default-toolchain $RUST_VERSION \
        --profile minimal \
        --component clippy \
        --component rustfmt \
        --component llvm-tools-preview
    RUN rustup toolchain install nightly \
        --component clippy \
        --component rustfmt \
        --component llvm-tools-preview
    RUN cargo install --locked --force --version 0.5.0 cargo-machete
    RUN cargo install --locked --force --version 0.17.6 cargo-audit
    RUN cargo install --locked --force --version 0.36.11 cargo-make
    RUN cargo install --locked --force --version 0.5.22 cargo-llvm-cov
    RUN cargo install --locked --force --version 0.1.61 cargo-chef
    RUN rustup --version
    RUN cargo --version
    RUN rustc --version

formatting-check:
    FROM +install-rust
    COPY Cargo.toml Cargo.toml
    COPY rustfmt.toml rustfmt.toml
    COPY crates/ crates/
    COPY sql-to-dbsp-compiler/lib sql-to-dbsp-compiler/lib
    RUN cargo +nightly fmt --all -- --check

install-python-deps:
    FROM +install-deps
    RUN pip install wheel
    COPY demo/demo_notebooks/requirements.txt requirements.txt
    RUN pip wheel -r requirements.txt --wheel-dir=wheels
    SAVE ARTIFACT wheels /wheels

install-python:
    FROM +install-deps
    COPY +install-python-deps/wheels wheels
    COPY demo/demo_notebooks/requirements.txt requirements.txt
    RUN pip install --user -v --no-index --find-links=wheels -r requirements.txt
    COPY python python
    RUN cd python && pip3 install --user -v ./feldera-api-client
    RUN cd python && pip3 install --user -v .
    SAVE ARTIFACT /root/.local/lib/python3.10
    SAVE ARTIFACT /root/.local/bin

build-webui-deps:
    FROM +install-deps
    COPY web-console/package.json ./web-console/package.json
    COPY web-console/yarn.lock ./web-console/yarn.lock

    RUN cd web-console && yarn install

build-webui:
    FROM +build-webui-deps
    COPY --dir web-console/public web-console/public
    COPY --dir web-console/src web-console/src
    COPY web-console/.editorconfig web-console/
    COPY web-console/.eslintrc.json web-console/
    COPY web-console/.prettierrc.js web-console/
    COPY web-console/next-env.d.ts ./web-console/next-env.d.ts
    COPY web-console/next.config.js ./web-console/next.config.js
    COPY web-console/next.d.ts ./web-console/next.d.ts
    COPY web-console/tsconfig.json ./web-console/tsconfig.json

    RUN cd web-console && yarn format:check
    RUN cd web-console && yarn build
    SAVE ARTIFACT ./web-console/out

prepare-cache:
    # We download and pre-build dependencies to cache it using cargo-chef.
    # See also (on why this is so complicated :/):
    # https://github.com/rust-lang/cargo/issues/2644
    # https://hackmd.io/@kobzol/S17NS71bh
    FROM +install-rust

    # We can't just copy crates from source, the reasons seem to have to do with
    # the way the hashes are computed for the cache: e.g., once
    # https://github.com/earthly/earthly/issues/786 is fixed this can go away
    # and crates can be copied instead:
    RUN mkdir -p .cargo
    RUN mkdir -p crates/dataflow-jit
    RUN mkdir -p crates/nexmark
    RUN mkdir -p crates/dbsp
    RUN mkdir -p crates/adapters
    RUN mkdir -p crates/pipeline_manager
    RUN mkdir -p sql-to-dbsp-compiler/lib/hashing
    RUN mkdir -p sql-to-dbsp-compiler/lib/readers
    RUN mkdir -p sql-to-dbsp-compiler/lib/sqllib
    RUN mkdir -p sql-to-dbsp-compiler/lib/sqlvalue
    RUN mkdir -p sql-to-dbsp-compiler/lib/tuple
    RUN mkdir -p sql-to-dbsp-compiler/temp
    #RUN mkdir -p crates/webui-tester

    COPY --keep-ts .cargo/config .cargo/config
    COPY --keep-ts Cargo.toml .
    COPY --keep-ts Cargo.lock .
    COPY --keep-ts crates/dataflow-jit/Cargo.toml crates/dataflow-jit/
    COPY --keep-ts crates/nexmark/Cargo.toml crates/nexmark/
    COPY --keep-ts crates/dbsp/Cargo.toml crates/dbsp/
    COPY --keep-ts crates/pipeline-types/Cargo.toml crates/pipeline-types/
    COPY --keep-ts crates/adapters/Cargo.toml crates/adapters/
    COPY --keep-ts crates/pipeline_manager/Cargo.toml crates/pipeline_manager/
    #COPY --keep-ts crates/webui-tester/Cargo.toml crates/webui-tester/
    COPY --keep-ts sql-to-dbsp-compiler/lib/hashing/Cargo.toml sql-to-dbsp-compiler/lib/hashing/
    COPY --keep-ts sql-to-dbsp-compiler/lib/readers/Cargo.toml sql-to-dbsp-compiler/lib/readers/
    COPY --keep-ts sql-to-dbsp-compiler/lib/sqllib/Cargo.toml sql-to-dbsp-compiler/lib/sqllib/
    COPY --keep-ts sql-to-dbsp-compiler/lib/sqlvalue/Cargo.toml sql-to-dbsp-compiler/lib/sqlvalue/
    COPY --keep-ts sql-to-dbsp-compiler/lib/tuple/Cargo.toml sql-to-dbsp-compiler/lib/tuple/
    COPY --keep-ts sql-to-dbsp-compiler/temp/Cargo.toml sql-to-dbsp-compiler/temp/

    RUN mkdir -p crates/dataflow-jit/src && touch crates/dataflow-jit/src/lib.rs
    RUN mkdir -p crates/nexmark/src && touch crates/nexmark/src/lib.rs
    RUN mkdir -p crates/dbsp/src && touch crates/dbsp/src/lib.rs
    RUN mkdir -p crates/dbsp/examples && touch crates/dbsp/examples/degrees.rs && touch crates/dbsp/examples/orgchart.rs
    RUN mkdir -p crates/pipeline-types/src && touch crates/pipeline-types/src/lib.rs
    RUN mkdir -p crates/adapters/src && touch crates/adapters/src/lib.rs
    RUN mkdir -p crates/adapters/examples && touch crates/adapters/examples/server.rs
    RUN mkdir -p crates/dataflow-jit/src && touch crates/dataflow-jit/src/main.rs
    RUN mkdir -p crates/nexmark/benches/nexmark-gen && touch crates/nexmark/benches/nexmark-gen/main.rs
    RUN mkdir -p crates/nexmark/benches/nexmark && touch crates/nexmark/benches/nexmark/main.rs
    RUN mkdir -p crates/dbsp/benches/gdelt && touch crates/dbsp/benches/gdelt/main.rs
    RUN mkdir -p crates/dbsp/benches/ldbc-graphalytics && touch crates/dbsp/benches/ldbc-graphalytics/main.rs
    RUN mkdir -p crates/dbsp/benches
    RUN touch crates/dbsp/benches/galen.rs
    RUN touch crates/dbsp/benches/fraud.rs
    RUN touch crates/dbsp/benches/path.rs
    RUN touch crates/dbsp/benches/consolidation.rs
    RUN touch crates/dbsp/benches/column_layer.rs
    RUN mkdir -p crates/pipeline_manager/src && touch crates/pipeline_manager/src/main.rs
    #RUN mkdir -p crates/webui-tester/src && touch crates/webui-tester/src/lib.rs
    RUN mkdir -p sql-to-dbsp-compiler/lib/hashing/src && touch sql-to-dbsp-compiler/lib/hashing/src/lib.rs
    RUN mkdir -p sql-to-dbsp-compiler/lib/readers/src && touch sql-to-dbsp-compiler/lib/readers/src/lib.rs
    RUN mkdir -p sql-to-dbsp-compiler/lib/sqllib/src && touch sql-to-dbsp-compiler/lib/sqllib/src/lib.rs
    RUN mkdir -p sql-to-dbsp-compiler/lib/sqlvalue/src && touch sql-to-dbsp-compiler/lib/sqlvalue/src/lib.rs
    RUN mkdir -p sql-to-dbsp-compiler/lib/tuple/src && touch sql-to-dbsp-compiler/lib/tuple/src/lib.rs

    ENV RUST_LOG=info
    RUN cargo chef prepare

    SAVE ARTIFACT --keep-ts recipe.json

build-cache:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +install-rust
    COPY --keep-ts +prepare-cache/recipe.json ./
    RUN cargo +$RUST_TOOLCHAIN chef cook $RUST_BUILD_PROFILE --workspace --all-targets
    RUN cargo +$RUST_TOOLCHAIN chef cook $RUST_BUILD_PROFILE --clippy --workspace
    # I have no clue why we need all these commands below to build all
    # dependencies (since we just did a build --workspace --all-targets). But if
    # we don't run all of them, it will go and compile dependencies during
    # the `build-*` targets (which should just compile our crates ideally to
    # maximize the cache benefit). This happens even without earthly (or docker)
    # if you just run cargo in our repo directly on clean target...
    #
    # The only issues for this I found was:
    # https://github.com/rust-lang/cargo/issues/2904
    # https://github.com/rust-lang/cargo/issues/6733
    #
    # When using `RUST_LOG=cargo::ops::cargo_rustc::fingerprint=info` to debug,
    # it looks like the hash for the crates changes.
    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --no-run
    RUN cargo +$RUST_TOOLCHAIN clippy $RUST_BUILD_PROFILE
    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package dbsp
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dbsp --no-run
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dbsp --features persistence --no-run
    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package pipeline_types
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package pipeline_types --no-run
    RUN cargo +$RUST_TOOLCHAIN clippy $RUST_BUILD_PROFILE --package pipeline_types
    RUN cargo +$RUST_TOOLCHAIN clippy $RUST_BUILD_PROFILE --package dbsp
    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package dbsp_adapters
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dbsp_adapters --no-run
    RUN cargo +$RUST_TOOLCHAIN clippy $RUST_BUILD_PROFILE --package dbsp_adapters
    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package pipeline-manager
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package pipeline-manager --no-run
    RUN cargo +$RUST_TOOLCHAIN clippy $RUST_BUILD_PROFILE --package pipeline-manager
    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package dataflow-jit
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dataflow-jit --no-run
    RUN cargo +$RUST_TOOLCHAIN clippy $RUST_BUILD_PROFILE --package dataflow-jit
    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package dbsp_nexmark
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dbsp_nexmark --no-run
    RUN cargo +$RUST_TOOLCHAIN clippy $RUST_BUILD_PROFILE --package dbsp_nexmark
    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package hashing
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package hashing --no-run
    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package readers
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package readers --no-run
    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package sqllib
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package sqllib --no-run
    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package sqlvalue
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package sqlvalue --no-run
    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package tuple
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package tuple --no-run

    # If we make this a workspace crate we can use the chef/cook commands but
    # it breaks `cargo build` in non-CI builds (because there is no
    # lib.rs and we don't want to check that in as it gets overwritten)
    COPY --dir sql-to-dbsp-compiler/temp sql-to-dbsp-compiler/temp
    RUN rm -f sql-to-dbsp-compiler/temp/src/lib.rs && touch sql-to-dbsp-compiler/temp/src/lib.rs
    RUN cd sql-to-dbsp-compiler/temp && cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --no-run

build-dbsp:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-cache --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE

    # Remove the skeleton created in build-cache
    RUN rm -rf crates/dbsp
    # Copy in the actual sources
    COPY --keep-ts --dir crates/dbsp crates/dbsp
    # pipeline-types is used by all subsequent dependencies. It's small enough and
    # easier to build it in this step instead of having a separate one.
    RUN rm -rf crates/pipeline-types
    COPY --keep-ts --dir crates/pipeline-types crates/pipeline-types
    COPY --keep-ts README.md README.md

    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package dbsp
    RUN cd crates/dbsp && cargo +$RUST_TOOLCHAIN machete
    RUN cargo +$RUST_TOOLCHAIN clippy $RUST_BUILD_PROFILE --package dbsp -- -D warnings
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dbsp --no-run

    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package pipeline_types
    RUN cargo +$RUST_TOOLCHAIN clippy $RUST_BUILD_PROFILE --package pipeline_types -- -D warnings
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package pipeline_types --no-run


build-dataflow-jit:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-dbsp --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE

    RUN rm -rf crates/dataflow-jit
    COPY --keep-ts --dir crates/dataflow-jit crates/dataflow-jit

    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package dataflow-jit
    RUN cd crates/dataflow-jit && cargo +$RUST_TOOLCHAIN machete
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dataflow-jit --no-run
    RUN cargo +$RUST_TOOLCHAIN clippy $RUST_BUILD_PROFILE --package dataflow-jit -- -D warnings

build-sql:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-dataflow-jit --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    COPY --keep-ts sql-to-dbsp-compiler sql-to-dbsp-compiler

    COPY demo/hello-world/combiner.sql demo/hello-world/combiner.sql
    COPY demo/project_demo00-SecOps/project.sql demo/project_demo00-SecOps/project.sql
    COPY demo/project_demo01-TimeSeriesEnrich/project.sql demo/project_demo01-TimeSeriesEnrich/project.sql
    COPY demo/project_demo02-FraudDetection/project.sql demo/project_demo02-FraudDetection/project.sql
    COPY demo/project_demo03-GreenTrip/project.sql demo/project_demo03-GreenTrip/project.sql
    COPY demo/project_demo04-SimpleSelect/project.sql demo/project_demo04-SimpleSelect/project.sql

    CACHE /root/.m2

    COPY sql-to-dbsp-compiler sql-to-dbsp-compiler
    RUN cd "sql-to-dbsp-compiler/SQL-compiler" && mvn package -DskipTests --no-transfer-progress
    SAVE ARTIFACT sql-to-dbsp-compiler/SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar sql2dbsp-jar-with-dependencies.jar
    SAVE ARTIFACT sql-to-dbsp-compiler

build-adapters:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    # Adapter integration tests use the SQL compiler.
    FROM +build-sql --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    RUN rm -rf crates/adapters
    COPY --keep-ts --dir crates/adapters crates/adapters

    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package dbsp_adapters --features="with-jit"
    RUN cd crates/adapters && cargo +$RUST_TOOLCHAIN machete
    RUN cargo +$RUST_TOOLCHAIN clippy $RUST_BUILD_PROFILE --package dbsp_adapters -- -D warnings
    ENV RUST_BACKTRACE=1

build-manager:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-adapters --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    # For some reason if this ENV before the FROM line it gets invalidated
    ENV WEBUI_BUILD_DIR=/dbsp/web-console/out
    COPY ( +build-webui/out ) ./web-console/out

    RUN rm -rf crates/pipeline_manager
    COPY --keep-ts --dir crates/pipeline_manager crates/pipeline_manager

    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package pipeline-manager
    RUN cd crates/pipeline_manager && cargo +$RUST_TOOLCHAIN machete
    RUN cargo +$RUST_TOOLCHAIN clippy $RUST_BUILD_PROFILE --package pipeline-manager -- -D warnings
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package pipeline-manager --no-run

    IF [ -f ./target/debug/pipeline-manager ]
        SAVE ARTIFACT --keep-ts ./target/debug/pipeline-manager pipeline-manager
        SAVE ARTIFACT --keep-ts ./target/debug/pipeline pipeline
    END
    IF [ -f ./target/release/pipeline-manager ]
        SAVE ARTIFACT --keep-ts ./target/release/pipeline-manager pipeline-manager
        SAVE ARTIFACT --keep-ts ./target/release/pipeline pipeline
    END

test-sql:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    # SQL-generated code imports adapters crate.
    FROM +build-adapters --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    RUN cd "sql-to-dbsp-compiler/SQL-compiler" && mvn package --no-transfer-progress

build-nexmark:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-dbsp --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE

    RUN rm -rf crates/nexmark
    COPY --keep-ts --dir crates/nexmark crates/nexmark

    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package dbsp_nexmark
    RUN cd crates/nexmark && cargo +$RUST_TOOLCHAIN machete
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dbsp_nexmark --no-run
    RUN cargo +$RUST_TOOLCHAIN clippy $RUST_BUILD_PROFILE --package dbsp_nexmark -- -D warnings

audit:
    FROM +build-cache
    RUN cargo audit || true

CARGO_TEST:
    COMMAND
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE
    ARG package
    ARG features
    ARG test_args
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package $package \
        $(if [ -z $features ]; then printf -- --features $features; fi) \
        -- $test_args

test-dbsp:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-dbsp --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    # Limit test execution to tests in trace::persistent::tests, because
    # executing everything takes too long and (in theory) the proptests we have
    # should ensure equivalence with the DRAM trace implementation:
    DO +CARGO_TEST \
        --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE \
        --package=dbsp --features=persistence --test_args=trace::persistent::tests
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dbsp

test-nexmark:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-nexmark --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dbsp_nexmark

test-dataflow-jit:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-dataflow-jit --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    # Tests use this demo directory
    COPY --keep-ts demo/project_demo01-TimeSeriesEnrich demo/project_demo01-TimeSeriesEnrich
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dataflow-jit

test-adapters:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-adapters --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    WITH DOCKER --pull docker.redpanda.com/vectorized/redpanda:v23.2.3
        RUN docker run -p 9092:9092 --rm -itd docker.redpanda.com/vectorized/redpanda:v23.2.3 \
            redpanda start --smp 2  && \
            sleep 5 && \
            cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dbsp_adapters --features="with-jit" --package sqllib
    END

test-manager:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-manager --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    ENV PGHOST=localhost
    ENV PGUSER=postgres
    ENV PGCLIENTENCODING=UTF8
    ENV PGPORT=5432
    ENV RUST_LOG=error
    WITH DOCKER --pull postgres
        # We just put the PGDATA in /dev/shm because the docker fs seems very slow (test time goes to 2min vs. shm 40s)
        RUN docker run --shm-size=512MB -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust -e PGDATA=/dev/shm -d postgres && \
            # Sleep until postgres is up (otherwise we get connection reset if we connect too early)
            # (See: https://github.com/docker-library/docs/blob/master/postgres/README.md#caveats)
            sleep 3 && \
            cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package pipeline-manager
    END
    # We keep the test binary around so we can run integration tests later. This incantation is used to find the
    # test binary path, adapted from: https://github.com/rust-lang/cargo/issues/3670
    RUN cp `cargo test --features integration-test --no-run --package pipeline-manager --message-format=json | jq -r 'select(.target.kind[0] == "lib") | .executable' | grep -v null` test_binary
    SAVE ARTIFACT test_binary

python-bindings-checker:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-manager --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    COPY +build-manager/pipeline-manager .
    RUN mkdir -p /root/.local/lib/python3.10
    RUN mkdir -p /root/.local/bin

    COPY +install-python/python3.10 /root/.local/lib/python3.10
    COPY +install-python/bin /root/.local/bin

    RUN pip3 install openapi-python-client==0.15.0 && openapi-python-client --version
    COPY +build-manager/pipeline-manager .
    COPY python/feldera-api-client feldera-api-client-base

    # This line will fail if the python bindings need to be regenerated
    RUN mkdir checker
    RUN cd checker && ../pipeline-manager --dump-openapi &&  \
        openapi-python-client generate --path openapi.json --fail-on-warning && \
        diff -bur feldera-api-client ../feldera-api-client-base


test-python:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-manager --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    COPY +build-manager/pipeline-manager .
    COPY +build-manager/pipeline .
    RUN mkdir -p /root/.local/lib/python3.10
    RUN mkdir -p /root/.local/bin

    COPY +install-python/python3.10 /root/.local/lib/python3.10
    COPY +install-python/bin /root/.local/bin

    COPY +build-sql/sql-to-dbsp-compiler sql-to-dbsp-compiler

    COPY demo/demo_notebooks demo/demo_notebooks
    COPY python/test.py python/test.py

    ENV PGHOST=localhost
    ENV PGUSER=postgres
    ENV PGCLIENTENCODING=UTF8
    ENV PGPORT=5432
    ENV RUST_LOG=error
    ENV WITH_POSTGRES=1
    ENV IN_CI=1
    WITH DOCKER --pull postgres
        RUN docker run --shm-size=512MB -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust -e PGDATA=/dev/shm -d postgres && \
            sleep 10 && \
            (./pipeline-manager --bind-address=0.0.0.0 --api-server-working-directory=/working-dir --compiler-working-directory=/working-dir --runner-working-directory=/working-dir --sql-compiler-home=/dbsp/sql-to-dbsp-compiler --dbsp-override-path=/dbsp --jit-pipeline-runner-path=/dbsp/pipeline --db-connection-string=postgresql://postgres:postgres@localhost:5432 &) && \
            sleep 5 && \
            python3 python/test.py && \
            cd demo/demo_notebooks && jupyter execute fraud_detection.ipynb --JupyterApp.log_level='DEBUG'
    END

test-rust:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    BUILD +test-dbsp --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    BUILD +test-nexmark --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    BUILD +test-adapters --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    BUILD +test-dataflow-jit --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    BUILD +test-manager --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE

# TODO: the following two container tasks duplicate work that we otherwise do in the Dockerfile,
# but by mostly repeating ourselves, we can reuse earlier Earthly stages to speed up the CI.
build-pipeline-manager-container:
    FROM +install-rust
    WORKDIR /

    # First, copy over the artifacts built from previous stages
    RUN mkdir -p database-stream-processor/sql-to-dbsp-compiler/SQL-compiler/target
    COPY +build-manager/pipeline-manager .
    COPY +build-manager/pipeline .
    COPY +build-sql/sql2dbsp-jar-with-dependencies.jar database-stream-processor/sql-to-dbsp-compiler/SQL-compiler/target/

    # Then copy over the crates needed by the sql compiler
    COPY crates/dbsp database-stream-processor/crates/dbsp
    COPY crates/pipeline-types database-stream-processor/crates/pipeline-types
    COPY crates/adapters database-stream-processor/crates/adapters
    COPY crates/dataflow-jit database-stream-processor/crates/dataflow-jit
    COPY README.md database-stream-processor/README.md

    # Then copy over the required SQL compiler files
    COPY sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp /database-stream-processor/sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp
    COPY sql-to-dbsp-compiler/lib /database-stream-processor/sql-to-dbsp-compiler/lib
    COPY sql-to-dbsp-compiler/temp /database-stream-processor/sql-to-dbsp-compiler/temp
    RUN ./pipeline-manager --bind-address=0.0.0.0 --api-server-working-directory=/working-dir --compiler-working-directory=/working-dir --runner-working-directory=/working-dir --sql-compiler-home=/database-stream-processor/sql-to-dbsp-compiler --dbsp-override-path=/database-stream-processor --precompile
    ENTRYPOINT ["./pipeline-manager", \
        "--bind-address=0.0.0.0", \
        "--api-server-working-directory=/working-dir", \
        "--compiler-working-directory=/working-dir", \
        "--runner-working-directory=/working-dir", \
        "--sql-compiler-home=/database-stream-processor/sql-to-dbsp-compiler", \
        "--dbsp-override-path=/database-stream-processor", \
        "--jit-pipeline-runner-path=/pipeline"]

# TODO: mirrors the Dockerfile. See note above.
build-demo-container:
    FROM +install-rust
    WORKDIR /
    RUN apt install unzip -y
    RUN arch=`dpkg --print-architecture`; \
            curl -LO https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-$arch.zip \
            && unzip rpk-linux-$arch.zip -d /bin/ \
            && rpk version \
            && rm rpk-linux-$arch.zip
    # Install snowsql
    RUN curl -O https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/1.2/linux_x86_64/snowsql-1.2.28-linux_x86_64.bash \
        && SNOWSQL_DEST=/bin SNOWSQL_LOGIN_SHELL=~/.profile bash snowsql-1.2.28-linux_x86_64.bash \
        && snowsql -v
    COPY +install-python/python3.10 /root/.local/lib/python3.10
    COPY +install-python/bin /root/.local/bin
    COPY demo demo
    CMD bash

build-kafka-connect-container:
    FROM DOCKERFILE -f deploy/Dockerfile --target kafka-connect .

test-docker-compose:
    FROM earthly/dind:alpine
    COPY deploy/docker-compose.yml .
    ENV FELDERA_VERSION=latest
    WITH DOCKER --pull postgres \
                --pull docker.redpanda.com/vectorized/redpanda:v23.2.3 \
                --load ghcr.io/feldera/pipeline-manager:latest=+build-pipeline-manager-container \
                --load ghcr.io/feldera/demo-container:latest=+build-demo-container
        RUN COMPOSE_HTTP_TIMEOUT=120 SECOPS_DEMO_ARGS="--prepare-args 200000" RUST_LOG=debug,tokio_postgres=info docker-compose -f docker-compose.yml --profile demo up --force-recreate --exit-code-from demo
    END

test-docker-compose-stable:
    FROM earthly/dind:alpine
    COPY deploy/docker-compose.yml .
    ENV FELDERA_VERSION=0.2.0
    WITH DOCKER --pull postgres \
                --pull docker.redpanda.com/vectorized/redpanda:v23.2.3 \
                --pull ghcr.io/feldera/pipeline-manager:0.2.0 \
                --pull ghcr.io/feldera/demo-container:0.2.0
        RUN COMPOSE_HTTP_TIMEOUT=120 SECOPS_DEMO_ARGS="--prepare-args 200000" RUST_LOG=debug,tokio_postgres=info docker-compose -f docker-compose.yml --profile demo up --force-recreate --exit-code-from demo
    END

test-debezium:
    FROM earthly/dind:alpine
    COPY deploy/docker-compose.yml .
    COPY deploy/docker-compose-debezium.yml .
    ENV FELDERA_VERSION=latest
    WITH DOCKER --pull postgres \
                --pull docker.redpanda.com/vectorized/redpanda:v23.2.3 \
                --pull debezium/example-mysql:2.3 \
                --load ghcr.io/feldera/pipeline-manager:latest=+build-pipeline-manager-container \
                --load ghcr.io/feldera/demo-container:latest=+build-demo-container \
                --load ghcr.io/feldera/kafka-connect:latest=+build-kafka-connect-container
        RUN COMPOSE_HTTP_TIMEOUT=120 RUST_LOG=debug,tokio_postgres=info docker-compose -f docker-compose.yml -f docker-compose-debezium.yml --profile debezium up --force-recreate --exit-code-from debezium-demo
    END

test-snowflake:
    FROM earthly/dind:alpine
    COPY deploy/docker-compose.yml .
    COPY deploy/.env .
    RUN cat .env
    ENV FELDERA_VERSION=latest
    WITH DOCKER --pull postgres \
                --pull docker.redpanda.com/vectorized/redpanda:v23.2.3 \
                --load ghcr.io/feldera/pipeline-manager:latest=+build-pipeline-manager-container \
                --load ghcr.io/feldera/demo-container:latest=+build-demo-container \
                --load ghcr.io/feldera/kafka-connect:latest=+build-kafka-connect-container
        RUN COMPOSE_HTTP_TIMEOUT=120 RUST_LOG=debug,tokio_postgres=info docker-compose --env-file .env -f docker-compose.yml --profile snowflake up --force-recreate --exit-code-from snowflake-demo
    END

# Fetches the test binary from test-manager, and produces a container image out of it
integration-test-container:
    FROM +install-deps
    COPY +test-manager/test_binary .
    # Check that the binary does indeed run integration tests
    RUN ./test_binary integration_test --list | grep integration_test
    ENV TEST_DBSP_URL=http://pipeline-manager:8080
    ENV RUST_BACKTRACE=1
    ENTRYPOINT ["./test_binary", "integration_test::"]
    SAVE IMAGE itest:latest

# Runs the integration test container against the docker compose setup
integration-tests:
    FROM earthly/dind:alpine
    COPY deploy/docker-compose.yml .
    COPY deploy/docker-compose-test.yml .
    COPY deploy/.env .
    ENV FELDERA_VERSION=latest
    WITH DOCKER --pull postgres \
                --load ghcr.io/feldera/pipeline-manager:latest=+build-pipeline-manager-container \
                --load test:latest=+integration-test-container
        RUN COMPOSE_HTTP_TIMEOUT=120 RUST_LOG=debug,tokio_postgres=info docker-compose --env-file .env -f docker-compose.yml -f docker-compose-test.yml up --force-recreate --exit-code-from test db pipeline-manager test
    END

benchmark:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-nexmark --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    COPY scripts/bench.bash scripts/bench.bash

    RUN bash scripts/bench.bash
    SAVE ARTIFACT crates/nexmark/nexmark_results.csv AS LOCAL .
    SAVE ARTIFACT crates/nexmark/dram_nexmark_results.csv AS LOCAL .
    SAVE ARTIFACT crates/nexmark/persistence_nexmark_results.csv AS LOCAL .
    SAVE ARTIFACT crates/dbsp/galen_results.csv AS LOCAL .
    SAVE ARTIFACT crates/dbsp/ldbc_results.csv AS LOCAL .

all-tests:
    BUILD +formatting-check
    BUILD +test-rust
    BUILD +test-python
    BUILD +audit
    BUILD +python-bindings-checker
    BUILD +test-sql
    BUILD +test-docker-compose
    BUILD +test-debezium
    BUILD +test-snowflake
    BUILD +integration-tests

