# The base image contains tools to build the code given that
# we need a Java and Rust compiler to run alongside the pipeline manager
# as of now. This will change later.
FROM ubuntu:24.04 AS base
ENV DEBIAN_FRONTEND=noninteractive

ARG TARGETOS
ARG TARGETARCH

# These two environment variables are used to make openssl-sys pick
# up libssl-dev and statically link it. Without it, our build defaults
# to building a vendored version of OpenSSL.
ENV OPENSSL_NO_VENDOR=1
ENV OPENSSL_STATIC=1
RUN apt update --fix-missing && apt install \
  # bindgen needs this (at least the dec crate uses bindgen)
  libclang-dev \
  # pkg-config is required for cargo to find libssl
  libssl-dev pkg-config \
  cmake \
  # rdkafka dependency needs libsasl2-dev and the CXX compiler
  libsasl2-dev build-essential \
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

# This user has the same UID and GID (1000) as the ci user on the machines so it helps with
# permissions when mounting volumes
USER ubuntu
WORKDIR /home/ubuntu

FROM base AS prebuilt
COPY build/pipeline-manager pipeline-manager
RUN mkdir -p lib/sql-to-dbsp-compiler/SQL-compiler/target
COPY build/sql2dbsp-jar-with-dependencies.jar lib/sql-to-dbsp-compiler/SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar

# Minimal image for running the pipeline manager
FROM prebuilt AS release

# Copy over `Cargo.lock` into home directory (copied overriding at each pipeline Rust compilation)
COPY --chown=ubuntu Cargo.lock Cargo.lock
COPY --chown=ubuntu Cargo.toml lib/Cargo.toml

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
  && mv mold-2.32.1-$arch-linux /home/ubuntu/mold \
  && rm mold-2.32.1-$arch-linux.tar.gz
ENV PATH="$PATH:/home/ubuntu/.cargo/bin:/home/ubuntu/mold/bin"
ENV RUSTFLAGS="-C link-arg=-fuse-ld=mold"

# Run the precompile phase to speed up Rust compilations during deployment
RUN ./pipeline-manager --bind-address=0.0.0.0 --sql-compiler-home=/home/ubuntu/lib/sql-to-dbsp-compiler --compilation-cargo-lock-path=/home/ubuntu/Cargo.lock --dbsp-override-path=/home/ubuntu/lib --precompile
RUN ./pipeline-manager --bind-address=0.0.0.0 --sql-compiler-home=/home/ubuntu/lib/sql-to-dbsp-compiler --compilation-cargo-lock-path=/home/ubuntu/Cargo.lock --dbsp-override-path=/home/ubuntu/lib --preinstall-pg-embed
ENV BANNER_ADDR=localhost
ENTRYPOINT ["./pipeline-manager", "--bind-address=0.0.0.0", "--sql-compiler-home=/home/ubuntu/lib/sql-to-dbsp-compiler", "--compilation-cargo-lock-path=/home/ubuntu/Cargo.lock", "--dbsp-override-path=/home/ubuntu/lib", "--demos-dir", "/home/ubuntu/demos"]