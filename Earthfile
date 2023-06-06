VERSION 0.7
FROM ubuntu:22.04

RUN apt-get update && apt-get install --yes sudo

WORKDIR /dbsp
ENV RUSTUP_HOME=$HOME/.rustup
ENV CARGO_HOME=$HOME/.cargo
# Adds python and rust binaries to thep path
ENV PATH=$HOME/.cargo/bin:$HOME/.local/bin:$PATH
ENV RUST_VERSION=1.69.0
ENV RUST_BUILD_MODE='' # set to --release for release builds

install-deps:
    RUN apt-get update
    RUN apt-get install --yes build-essential curl libssl-dev build-essential pkg-config \
                              cmake git gcc clang libclang-dev python3-pip python3-plumbum \
                              hub numactl openjdk-19-jre-headless maven netcat jq \
                              libsasl2-dev docker.io libenchant-2-2 graphviz
    RUN curl -fsSL https://deb.nodesource.com/setup_19.x | sudo -E bash - && apt-get install -y nodejs
    RUN apt-get install -y
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
    RUN cargo install --force --version 0.5.0 cargo-machete
    RUN cargo install --force --version 0.17.6 cargo-audit
    RUN cargo install --force --version 0.36.7 cargo-make
    RUN cargo install --force --version 0.5.19 cargo-llvm-cov
    RUN cargo install --force  --version 0.1.61 cargo-chef
    RUN rustup --version
    RUN cargo --version
    RUN rustc --version

install-python-deps:
    FROM +install-deps
    COPY sql-to-dbsp-compiler/doc/requirements.txt requirements-sql-doc.txt
    RUN pip3 install -r requirements-sql-doc.txt
    COPY demo/demo_notebooks/requirements.txt requirements-dbsp-demo.txt
    RUN pip3 install -r requirements-dbsp-demo.txt

build-webui-deps:
    FROM +install-deps
    COPY web-ui/package.json ./web-ui/package.json
    COPY web-ui/yarn.lock ./web-ui/yarn.lock

    RUN cd web-ui && yarn install

build-webui:
    FROM +build-webui-deps
    COPY --dir web-ui/public web-ui/public
    COPY --dir web-ui/src web-ui/src
    COPY --dir web-ui/styles web-ui/styles
    COPY web-ui/.editorconfig web-ui/
    COPY web-ui/.eslintrc.json web-ui/
    COPY web-ui/.prettierrc.js web-ui/
    COPY web-ui/next-env.d.ts ./web-ui/next-env.d.ts
    COPY web-ui/next.config.js ./web-ui/next.config.js
    COPY web-ui/next.d.ts ./web-ui/next.d.ts
    COPY web-ui/tsconfig.json ./web-ui/tsconfig.json

    RUN cd web-ui && yarn format:check
    RUN cd web-ui && yarn build
    RUN cd web-ui && yarn export
    SAVE ARTIFACT ./web-ui/out

prepare-cache:
    # We download and pre-build dependencies to cache it using cargo-chef.
    # See also (on why this is so complicated :/):
    # https://github.com/rust-lang/cargo/issues/2644
    # https://hackmd.io/@kobzol/S17NS71bh
    FROM +install-rust

    RUN mkdir -p .cargo
    RUN mkdir -p crates/dataflow-jit
    RUN mkdir -p crates/nexmark
    RUN mkdir -p crates/dbsp
    RUN mkdir -p crates/adapters
    RUN mkdir -p crates/pipeline_manager
    RUN mkdir -p sql-to-dbsp-compiler/lib/genlib
    RUN mkdir -p sql-to-dbsp-compiler/lib/hashing
    RUN mkdir -p sql-to-dbsp-compiler/lib/readers
    RUN mkdir -p sql-to-dbsp-compiler/lib/sqllib
    RUN mkdir -p sql-to-dbsp-compiler/lib/sqlvalue
    RUN mkdir -p sql-to-dbsp-compiler/lib/tuple
    #RUN mkdir -p crates/webui-tester

    COPY --keep-ts .cargo/config .cargo/config
    COPY --keep-ts Cargo.toml .
    COPY --keep-ts Cargo.lock .
    COPY --keep-ts crates/dataflow-jit/Cargo.toml crates/dataflow-jit/
    COPY --keep-ts crates/nexmark/Cargo.toml crates/nexmark/
    COPY --keep-ts crates/dbsp/Cargo.toml crates/dbsp/
    COPY --keep-ts crates/adapters/Cargo.toml crates/adapters/
    COPY --keep-ts crates/pipeline_manager/Cargo.toml crates/pipeline_manager/
    COPY --keep-ts sql-to-dbsp-compiler/lib/genlib/Cargo.toml sql-to-dbsp-compiler/lib/genlib/
    COPY --keep-ts sql-to-dbsp-compiler/lib/hashing/Cargo.toml sql-to-dbsp-compiler/lib/hashing/
    COPY --keep-ts sql-to-dbsp-compiler/lib/readers/Cargo.toml sql-to-dbsp-compiler/lib/readers/
    COPY --keep-ts sql-to-dbsp-compiler/lib/sqllib/Cargo.toml sql-to-dbsp-compiler/lib/sqllib/
    COPY --keep-ts sql-to-dbsp-compiler/lib/sqlvalue/Cargo.toml sql-to-dbsp-compiler/lib/sqlvalue/
    COPY --keep-ts sql-to-dbsp-compiler/lib/tuple/Cargo.toml sql-to-dbsp-compiler/lib/tuple/
    #COPY --keep-ts crates/webui-tester/Cargo.toml crates/webui-tester/

    RUN mkdir -p crates/dataflow-jit/src && touch crates/dataflow-jit/src/lib.rs
    RUN mkdir -p crates/nexmark/src && touch crates/nexmark/src/lib.rs
    RUN mkdir -p crates/dbsp/src && touch crates/dbsp/src/lib.rs
    RUN mkdir -p crates/dbsp/examples && touch crates/dbsp/examples/degrees.rs && touch crates/dbsp/examples/orgchart.rs
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
    RUN mkdir -p sql-to-dbsp-compiler/lib/genlib/src && touch sql-to-dbsp-compiler/lib/genlib/src/lib.rs
    RUN mkdir -p sql-to-dbsp-compiler/lib/hashing/src && touch sql-to-dbsp-compiler/lib/hashing/src/lib.rs
    RUN mkdir -p sql-to-dbsp-compiler/lib/readers/src && touch sql-to-dbsp-compiler/lib/readers/src/lib.rs
    RUN mkdir -p sql-to-dbsp-compiler/lib/sqllib/src && touch sql-to-dbsp-compiler/lib/sqllib/src/lib.rs
    RUN mkdir -p sql-to-dbsp-compiler/lib/sqlvalue/src && touch sql-to-dbsp-compiler/lib/sqlvalue/src/lib.rs
    RUN mkdir -p sql-to-dbsp-compiler/lib/tuple/src && touch sql-to-dbsp-compiler/lib/tuple/src/lib.rs

    ENV RUST_LOG=info
    RUN cargo chef prepare

    SAVE ARTIFACT --keep-ts --keep-own recipe.json

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
    RUN cargo +$RUST_TOOLCHAIN clippy $RUST_BUILD_PROFILE --package dbsp
    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package dbsp_adapters
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dbsp_adapters --no-run
    RUN cargo +$RUST_TOOLCHAIN clippy $RUST_BUILD_PROFILE --package dbsp_adapters
    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package dbsp_pipeline_manager
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dbsp_pipeline_manager --no-run
    RUN cargo +$RUST_TOOLCHAIN clippy $RUST_BUILD_PROFILE --package dbsp_pipeline_manager
    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package dataflow-jit
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dataflow-jit --no-run
    RUN cargo +$RUST_TOOLCHAIN clippy $RUST_BUILD_PROFILE --package dataflow-jit
    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package dbsp_nexmark
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dbsp_nexmark --no-run
    RUN cargo +$RUST_TOOLCHAIN clippy $RUST_BUILD_PROFILE --package dbsp_nexmark
    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package genlib
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package genlib --no-run
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

    SAVE ARTIFACT --keep-ts --keep-own $CARGO_HOME
    SAVE ARTIFACT --keep-ts --keep-own ./target

build-dbsp:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-cache --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE

    # Remove the skeleton created in build-cache
    RUN rm -rf crates/dbsp
    # Copy in the actual sources
    COPY --keep-ts --dir crates/dbsp crates/dbsp
    COPY --keep-ts README.md README.md

    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package dbsp
    RUN cd crates/dbsp && cargo +$RUST_TOOLCHAIN machete
    RUN cargo +$RUST_TOOLCHAIN clippy $RUST_BUILD_PROFILE --package dbsp -- -D warnings
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dbsp --no-run
    # Update target folder for future tasks, the whole --keep-ts, --keep-own
    # args were an attempt in fixing the dependency problem (see build-cache)
    # but it doesn't seem to make a difference.
    SAVE ARTIFACT --keep-ts --keep-own ./target/* ./target

build-adapters:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-dbsp --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    RUN rm -rf crates/adapters
    COPY --keep-ts --dir crates/adapters crates/adapters

    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package dbsp_adapters
    RUN cd crates/adapters && cargo +$RUST_TOOLCHAIN machete
    RUN cargo +$RUST_TOOLCHAIN clippy $RUST_BUILD_PROFILE --package dbsp_adapters -- -D warnings
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dbsp_adapters --no-run

    SAVE ARTIFACT --keep-ts --keep-own ./target/* ./target

build-manager:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-adapters --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    # For some reason if this ENV before the FROM line it gets invalidated
    ENV WEBUI_BUILD_DIR=/dbsp/web-ui/out
    COPY ( +build-webui/out ) ./web-ui/out

    RUN rm -rf crates/pipeline_manager
    COPY --keep-ts --dir crates/pipeline_manager crates/pipeline_manager
    COPY --keep-ts python python

    RUN cargo +$RUST_TOOLCHAIN build $RUST_BUILD_PROFILE --package dbsp_pipeline_manager
    RUN cd crates/pipeline_manager && cargo +$RUST_TOOLCHAIN machete
    RUN cargo +$RUST_TOOLCHAIN clippy $RUST_BUILD_PROFILE --package dbsp_pipeline_manager -- -D warnings
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dbsp_pipeline_manager --no-run
    RUN cargo make -e RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE --cwd crates/pipeline_manager/ openapi_python
    IF [ -f ./target/debug/dbsp_pipeline_manager ]
        SAVE ARTIFACT --keep-ts --keep-own ./target/debug/dbsp_pipeline_manager dbsp_pipeline_manager
    END
    IF [ -f ./target/release/dbsp_pipeline_manager ]
        SAVE ARTIFACT --keep-ts --keep-own ./target/release/dbsp_pipeline_manager dbsp_pipeline_manager
    END
    SAVE ARTIFACT --keep-ts --keep-own ./python python

build-sql:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    # TODO: does not need to depend on build-manager, instead save and copy
    # build artefacts in downstream targets
    FROM +build-manager --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    COPY --keep-ts sql-to-dbsp-compiler sql-to-dbsp-compiler
    COPY --keep-ts crates crates

    CACHE /root/.m2
    COPY sql-to-dbsp-compiler sql-to-dbsp-compiler
    RUN cd "sql-to-dbsp-compiler/SQL-compiler" && mvn -DskipTests package
    SAVE ARTIFACT sql-to-dbsp-compiler/SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar sql2dbsp-jar-with-dependencies.jar

build-sql-docs:
    FROM +install-python-deps
    COPY sql-to-dbsp-compiler/doc/ sql-to-dbsp-compiler/doc/
    RUN doc8 --ignore-path _build --max-line-length 120 .
    RUN cd sql-to-dbsp-compiler/doc/ && sphinx-build -M html "." "_build"
    SAVE ARTIFACT sql-to-dbsp-compiler/doc/_build/html AS LOCAL sql-docs

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
    SAVE ARTIFACT --keep-ts --keep-own ./target/* ./target

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

test-dbsp:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-dbsp --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    RUN cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dbsp

test-nexmark:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-dbsp --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
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
    WITH DOCKER --pull docker.redpanda.com/vectorized/redpanda:v22.3.11
        RUN docker run -p 9092:9092 --rm -itd docker.redpanda.com/vectorized/redpanda:v22.3.11 \
            redpanda start --smp 2 && \
            cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dbsp_adapters
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
        RUN docker run --shm-size=256MB -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust -e PGDATA=/dev/shm -d postgres && \
            # Sleep until postgres is up (otherwise we get connection reset if we connect too early)
            # (See: https://github.com/docker-library/docs/blob/master/postgres/README.md#caveats)
            sleep 3 && \
            cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dbsp_pipeline_manager
    END

test-python:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-sql --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    COPY --keep-ts scripts scripts
    COPY --keep-ts demo/demo_notebooks demo/demo_notebooks
    ENV PGHOST=localhost
    ENV PGUSER=postgres
    ENV PGCLIENTENCODING=UTF8
    ENV PGPORT=5432
    ENV RUST_LOG=error
    ENV WITH_POSTGRES=1
    WITH DOCKER --pull postgres
        # We just put the PGDATA in /dev/shm because the docker fs seems very slow (test time goes to 2min vs. shm 40s)
        RUN docker run --shm-size=256MB -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust -e PGDATA=/dev/shm -d postgres && \
            sleep 3 && \
            cargo make -e RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE --cwd crates/pipeline_manager/ python_test
    END

test-rust:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    BUILD +test-dbsp --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    BUILD +test-nexmark --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    BUILD +test-adapters --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    BUILD +test-dataflow-jit --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    BUILD +test-manager --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE

test-sql:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE
    FROM +build-sql --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    RUN cd "sql-to-dbsp-compiler/SQL-compiler" && mvn test

# TODO: the following two container tasks duplicate work that we otherwise do in the Dockerfile,
# but by mostly repeating ourselves, we can reuse earlier Earthly stages to speed up the CI.
build-dbsp-manager-container:
    FROM +install-rust
    WORKDIR /

    # First, copy over the artifacts built from previous stages
    RUN mkdir -p database-stream-processor/sql-to-dbsp-compiler/SQL-compiler/target
    COPY +build-manager/dbsp_pipeline_manager .
    COPY +build-sql/sql2dbsp-jar-with-dependencies.jar database-stream-processor/sql-to-dbsp-compiler/SQL-compiler/target/

    # Then copy over the crates needed by the sql compiler
    COPY crates/dbsp database-stream-processor/crates/dbsp
    COPY crates/adapters database-stream-processor/crates/adapters
    COPY crates/dataflow-jit database-stream-processor/crates/dataflow-jit
    COPY README.md database-stream-processor/README.md

    # Then copy over the required SQL compiler files
    COPY sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp /database-stream-processor/sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp
    COPY sql-to-dbsp-compiler/lib /database-stream-processor/sql-to-dbsp-compiler/lib
    COPY sql-to-dbsp-compiler/temp /database-stream-processor/sql-to-dbsp-compiler/temp
    CMD ./dbsp_pipeline_manager --bind-address=0.0.0.0 --working-directory=/working-dir --sql-compiler-home=/database-stream-processor/sql-to-dbsp-compiler --dbsp-override-path=/database-stream-processor
    SAVE IMAGE ghcr.io/feldera/dbsp-manager

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
    COPY +build-manager/dbsp_pipeline_manager .
    RUN ./dbsp_pipeline_manager --dump-openapi
    RUN pip3 install openapi-python-client websockets
    COPY python python
    RUN cd python &&  \
        openapi-python-client generate --path ../openapi.json && \
        pip3 install ./dbsp-api-client && \
        pip3 install .
    COPY demo demo
    CMD bash
    SAVE IMAGE ghcr.io/feldera/demo-container

test-docker-compose:
    FROM earthly/dind:alpine
    COPY deploy/docker-compose.yml .
    WITH DOCKER --load ghcr.io/feldera/dbsp-manager=+build-dbsp-manager-container \
                --load ghcr.io/feldera/demo-container=+build-demo-container
        RUN SECOPS_DEMO_ARGS="--prepare-args 500000" docker-compose -f docker-compose.yml --profile demo up --force-recreate --exit-code-from demo
    END

all-tests:
    BUILD +test-rust
    BUILD +test-python
    BUILD +audit
    BUILD +test-sql
    BUILD +test-docker-compose