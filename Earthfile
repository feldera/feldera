VERSION 0.7
FROM ubuntu:22.04

RUN apt-get update && apt-get install --yes sudo

WORKDIR /dbsp
ENV RUSTUP_HOME=$HOME/.rustup
ENV CARGO_HOME=$HOME/.cargo
ENV PATH=$HOME/.cargo/bin:$PATH
ENV RUST_VERSION=1.69.0

install-deps:
    RUN apt-get update
    RUN apt-get install --yes build-essential curl libssl-dev build-essential pkg-config \
                              cmake git gcc clang libclang-dev python3-pip python3-plumbum \
                              hub numactl cmake openjdk-19-jre-headless maven netcat jq \
                              libsasl2-dev docker.io

install-rust:
    FROM +install-deps
    RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- \
        -y \
        --default-toolchain $RUST_VERSION \
        --profile minimal \
        --component clippy \
        --component rustfmt \
        --component llvm-tools-preview
    RUN chmod -R a+w $RUSTUP_HOME $CARGO_HOME
    RUN rustup --version
    RUN cargo --version
    RUN rustc --version

install-nextjs:
    RUN sudo apt-get update
    RUN sudo apt-get install git sudo curl
    RUN curl -fsSL https://deb.nodesource.com/setup_19.x | sudo -E bash - &&\
    RUN sudo apt-get install -y nodejs
    RUN npm install --global yarn
    RUN npm install --global openapi-typescript-codegen

install-chef:
    FROM +install-rust
    RUN cargo install --debug cargo-chef

prepare-cache:
    # We download and pre-build dependencies to cache it using cargo-chef.
    # See also (on why this is so complicated :/):
    # https://github.com/rust-lang/cargo/issues/2644
    # https://hackmd.io/@kobzol/S17NS71bh
    FROM +install-chef

    RUN mkdir -p crates/dataflow-jit
    RUN mkdir -p crates/nexmark
    RUN mkdir -p crates/dbsp
    RUN mkdir -p crates/adapters
    RUN mkdir -p crates/pipeline_manager
    #RUN mkdir -p crates/webui-tester

    COPY --keep-ts Cargo.toml .
    COPY --keep-ts Cargo.lock .
    COPY --keep-ts crates/dataflow-jit/Cargo.toml crates/dataflow-jit/
    COPY --keep-ts crates/nexmark/Cargo.toml crates/nexmark/
    COPY --keep-ts crates/dbsp/Cargo.toml crates/dbsp/
    COPY --keep-ts crates/adapters/Cargo.toml crates/adapters/
    COPY --keep-ts crates/pipeline_manager/Cargo.toml crates/pipeline_manager/
    #COPY --keep-ts crates/webui-tester/Cargo.toml crates/webui-tester/

    RUN mkdir -p crates/dataflow-jit/src && touch crates/dataflow-jit/src/lib.rs
    RUN mkdir -p crates/nexmark/src && touch crates/nexmark/src/lib.rs
    RUN mkdir -p crates/dbsp/src && touch crates/dbsp/src/lib.rs
    RUN mkdir -p crates/adapters/src && touch crates/adapters/src/lib.rs
    RUN mkdir -p crates/dataflow-jit/src && touch crates/dataflow-jit/src/main.rs
    RUN mkdir -p crates/nexmark/benches/nexmark-gen && touch crates/nexmark/benches/nexmark-gen/main.rs
    RUN mkdir -p crates/nexmark/benches/nexmark && touch crates/nexmark/benches/nexmark/main.rs
    RUN mkdir -p crates/dbsp/benches/gdelt && touch crates/dbsp/benches/gdelt/main.rs
    RUN mkdir -p crates/dbsp/benches/ldbc-graphalytics && touch crates/dbsp/benches/ldbc-graphalytics/main.rs
    RUN mkdir -p crates/pipeline_manager/src && touch crates/pipeline_manager/src/main.rs
    #RUN mkdir -p crates/webui-tester/src && touch crates/webui-tester/src/lib.rs

    ENV RUST_LOG=info
    RUN cargo chef prepare

    SAVE ARTIFACT --keep-ts --keep-own recipe.json

build-cache:
    FROM +install-chef
    COPY --keep-ts +prepare-cache/recipe.json ./
    RUN cargo chef cook --workspace --all-targets
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
    RUN cargo build
    RUN cargo test --no-run
    RUN cargo build --package dbsp
    RUN cargo test --package dbsp --no-run
    RUN cargo build --package dbsp_adapters
    RUN cargo test --package dbsp_adapters --no-run
    RUN cargo build --package dbsp_pipeline_manager
    RUN cargo test --package dbsp_pipeline_manager --no-run
    RUN cargo build --package dataflow-jit
    RUN cargo test --package dataflow-jit --no-run
    RUN cargo build --package dbsp_nexmark
    RUN cargo test --package dbsp_nexmark --no-run

    SAVE ARTIFACT --keep-ts --keep-own $CARGO_HOME
    SAVE ARTIFACT --keep-ts --keep-own ./target

build-dbsp:
    FROM +build-cache

    # Remove the skeleton created in build-cache
    RUN rm -rf crates/dbsp
    # Copy in the actual sources
    COPY --keep-ts --dir crates/dbsp crates/dbsp
    COPY --keep-ts README.md README.md

    RUN cargo build --package dbsp
    RUN cargo test --package dbsp --no-run
    # Update target folder for future tasks, the whole --keep-ts, --keep-own
    # args were an attempt in fixing the dependency problem (see build-cache)
    # but it doesn't seem to make a difference.
    SAVE ARTIFACT --keep-ts --keep-own ./target/* ./target


build-adapters:
    FROM +build-dbsp

    RUN rm -rf crates/adapters
    COPY --keep-ts --dir crates/adapters crates/adapters

    RUN cargo build --package dbsp_adapters
    RUN cargo test --package dbsp_adapters --no-run
    SAVE ARTIFACT --keep-ts --keep-own ./target/* ./target

build-manager:
    FROM +build-adapters

    RUN rm -rf crates/pipeline_manager
    COPY --keep-ts --dir crates/pipeline_manager crates/pipeline_manager

    RUN cargo build --package dbsp_pipeline_manager
    RUN cargo test --package dbsp_pipeline_manager --no-run
    SAVE ARTIFACT --keep-ts --keep-own ./target/* ./target

build-dataflow-jit:
    FROM +build-dbsp

    RUN rm -rf crates/dataflow-jit
    COPY --keep-ts --dir crates/dataflow-jit crates/dataflow-jit

    RUN cargo build --package dataflow-jit
    RUN cargo test --package dataflow-jit --no-run
    SAVE ARTIFACT --keep-ts --keep-own ./target/* ./target

build-nexmark:
    FROM +build-dbsp

    RUN rm -rf crates/nexmark
    COPY --keep-ts --dir crates/nexmark crates/nexmark

    RUN cargo build --package dbsp_nexmark
    RUN cargo test --package dbsp_nexmark --no-run
    SAVE ARTIFACT --keep-ts --keep-own ./target/* ./target

test-dbsp:
    FROM +build-dbsp
    RUN cargo test --package dbsp

test-dataflow-jit:
    FROM +build-dataflow-jit
    # Tests use this demo directory
    COPY --keep-ts demo/project_demo01-TimeSeriesEnrich demo/project_demo01-TimeSeriesEnrich
    RUN cargo test --package dataflow-jit

test-adapters:
    FROM +build-adapters
    WITH DOCKER --pull docker.redpanda.com/vectorized/redpanda:v22.3.11
        RUN docker run --name redpanda -p 9092:9092 --rm -itd docker.redpanda.com/vectorized/redpanda:v22.3.11 && \
            cargo test --package dbsp_adapters
    END

test-manager:
    FROM +build-manager
    ENV PGHOST=localhost
    ENV PGUSER=postgres
    ENV PGCLIENTENCODING=UTF8
    ENV PGPORT=5432
    ENV RUST_LOG=error
    WITH DOCKER --pull postgres
        # We just put the PGDATA in /dev/shm because the docker fs seems very slow (test time goes to 2min vs. shm 40s)
        RUN docker run --shm-size=256MB -p 5432:5432 --name postgres -e POSTGRES_HOST_AUTH_METHOD=trust -e PGDATA=/dev/shm -d postgres && \
            # Sleep until postgres is up (otherwise we get connection reset if we connect too early)
            # (See: https://github.com/docker-library/docs/blob/master/postgres/README.md#caveats)
            sleep 3 && \
            cargo test --package dbsp_pipeline_manager
    END