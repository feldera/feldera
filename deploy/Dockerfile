# The base image contains tools to build the code given that
# we need a Java and Rust compiler to run alongside the pipeline manager
# as of now. This will change later.
FROM ubuntu:24.04 AS base
ENV DEBIAN_FRONTEND=noninteractive

# These two environment variables are used to make openssl-sys pick
# up libssl-dev and statically link it. Without it, our build defaults
# to building a vendored version of OpenSSL.
ENV OPENSSL_NO_VENDOR=1
ENV OPENSSL_STATIC=1
RUN apt update --fix-missing && apt install \
  # pkg-config is required for cargo to find libssl
  libssl-dev pkg-config \
  # rdkafka dependency needs cmake and a CXX compiler
  cmake build-essential \
  # To install rust
  curl  \
  # For running the SQL compiler
  openjdk-21-jre-headless -y \
  # Install locale-gen
  locales \
  # To add the nodesource debian repository
  ca-certificates gnupg \
  # Required by the `metrics-exporter-tcp` crate
  protobuf-compiler

# Set UTF-8 locale. Needed for the Rust compiler to handle Unicode column names.
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
  locale-gen
ENV LC_ALL=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en

# Use cargo-chef to produce a recipe.json file
# to cache the requisite dependencies
FROM base AS chef
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.83.0
RUN /root/.cargo/bin/cargo install cargo-chef
WORKDIR app

# Build web-ui
FROM base AS web-ui-builder

## Install Bun.js
RUN apt install unzip -y
RUN curl -fsSL https://bun.sh/install | bash -s "bun-v1.2.2"
ENV PATH="$HOME/.bun/bin:$PATH"
RUN ln -s $HOME/.bun/bin/bun /usr/bin/bun

# sveltekit
COPY web-console/package.json web-console/
COPY web-console/bun.lockb web-console/
RUN cd web-console && bun install

COPY web-console/static web-console/static
COPY web-console/src web-console/src
COPY web-console/.prettierignore web-console/
COPY web-console/.prettierrc web-console/
COPY web-console/eslint.config.js web-console/
COPY web-console/postcss.config.js web-console/
COPY web-console/svelte.config.js web-console/
COPY web-console/tailwind.config.ts web-console/
COPY web-console/tsconfig.json web-console/
COPY web-console/vite.config.ts web-console/
RUN cd web-console && bun run build

# Cache dependencies from rust
FROM chef AS planner
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY crates crates
RUN mkdir sql-to-dbsp-compiler
COPY sql-to-dbsp-compiler/lib sql-to-dbsp-compiler/lib
RUN /root/.cargo/bin/cargo chef prepare --recipe-path recipe.json

# Use the recipe.json file to build dependencies first and cache that
# layer for faster incremental builds of source-code only changes
FROM chef AS builder
ENV CARGO_INCREMENTAL=0
# Making gcc, installed as a part of build-essentials, a default C compiler to build Rust dependencies
ENV CC=gcc-13
COPY --from=planner /app/recipe.json recipe.json
RUN /root/.cargo/bin/cargo chef cook --release --recipe-path recipe.json --bin=pipeline-manager --features=pg-embed
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY crates crates
RUN mkdir sql-to-dbsp-compiler || true
COPY sql-to-dbsp-compiler/lib sql-to-dbsp-compiler/lib
COPY --from=web-ui-builder /web-console/build web-console-build
ENV WEBCONSOLE_BUILD_DIR=/app/web-console-build
ARG FELDERA_PLATFORM_VERSION_SUFFIX=""
RUN /root/.cargo/bin/cargo build --release --bin=pipeline-manager --features=pg-embed

# Java build can be performed in parallel. Outputs should be architecture independent
FROM base AS javabuild
RUN apt install maven git -y
RUN mkdir sql
COPY sql-to-dbsp-compiler /sql/sql-to-dbsp-compiler
RUN --mount=type=cache,target=/root/.m2 \
    cd /sql/sql-to-dbsp-compiler && ./build.sh

# Minimal image for running the pipeline manager
FROM base AS release
# Pipeline manager binary
RUN useradd -ms /bin/bash feldera
USER feldera
WORKDIR /home/feldera
COPY --from=builder /app/target/release/pipeline-manager pipeline-manager
# SQL compiler uber jar
RUN mkdir -p lib/sql-to-dbsp-compiler/SQL-compiler/target
COPY --from=javabuild /sql/sql-to-dbsp-compiler/SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar lib/sql-to-dbsp-compiler/SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar

# Copy over `Cargo.lock` into home directory (copied overriding at each pipeline Rust compilation)
COPY --chown=feldera Cargo.lock Cargo.lock

# Copy over demos
RUN mkdir -p demos
COPY demo/packaged/sql demos

# The crates needed for the SQL compiler
COPY crates/ lib/crates/
COPY README.md lib/README.md
RUN mkdir -p lib/sql-to-dbsp-compiler/lib

# Copy over the rust code and sql-to-dbsp script
COPY sql-to-dbsp-compiler/lib lib/sql-to-dbsp-compiler/lib
COPY sql-to-dbsp-compiler/temp lib/sql-to-dbsp-compiler/temp
COPY sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp lib/sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp

# Install cargo and rust for this non-root user
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.83.0
# The download URL for mold uses x86_64/aarch64 whereas dpkg --print-architecture says amd64/arm64
RUN arch=`dpkg --print-architecture | sed "s/arm64/aarch64/g" | sed "s/amd64/x86_64/g"`; \
  curl -LO https://github.com/rui314/mold/releases/download/v2.32.1/mold-2.32.1-$arch-linux.tar.gz \
  && tar -xzvf mold-2.32.1-$arch-linux.tar.gz \
  && mv mold-2.32.1-$arch-linux /home/feldera/mold \
  && rm mold-2.32.1-$arch-linux.tar.gz
ENV PATH="$PATH:/home/feldera/.cargo/bin:/home/feldera/mold/bin"
ENV RUSTFLAGS="-C link-arg=-fuse-ld=mold"
# Run the precompile phase to speed up Rust compilations during deployment
RUN ./pipeline-manager --bind-address=0.0.0.0 --sql-compiler-home=/home/feldera/lib/sql-to-dbsp-compiler --compilation-cargo-lock-path=/home/feldera/Cargo.lock --dbsp-override-path=/home/feldera/lib --precompile
# Run the pg-embed preinstall such that it doesn't need to download the Postgres binaries afterward
RUN ./pipeline-manager --bind-address=0.0.0.0 --sql-compiler-home=/home/feldera/lib/sql-to-dbsp-compiler --compilation-cargo-lock-path=/home/feldera/Cargo.lock --dbsp-override-path=/home/feldera/lib --preinstall-pg-embed
ENV BANNER_ADDR=localhost
ENTRYPOINT ["./pipeline-manager", "--bind-address=0.0.0.0", "--sql-compiler-home=/home/feldera/lib/sql-to-dbsp-compiler", "--compilation-cargo-lock-path=/home/feldera/Cargo.lock", "--dbsp-override-path=/home/feldera/lib", "--demos-dir", "/home/feldera/demos"]
