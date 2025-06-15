# The base image contains tools to build the code given that
# we need a Java and Rust compiler to run alongside the pipeline manager
# as of now. This will change later.
FROM ubuntu:24.04 AS base
ENV DEBIAN_FRONTEND=noninteractive

# These two environment variables are used to make openssl-sys pick
# up libssl-dev and statically link it. Without it, our build defaults
# to building a vendored version of OpenSSL.
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
  protobuf-compiler \
  # Otherwise, postgresql_embedded complains when it tries to set timezeone to UTC
  tzdata

# Set UTF-8 locale. Needed for the Rust compiler to handle Unicode column names.
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
  locale-gen

USER ubuntu
WORKDIR /home/ubuntu

ENV OPENSSL_NO_VENDOR=1
ENV OPENSSL_STATIC=1
ENV LC_ALL=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en

# This user has the same UID and GID (1000) as the ci user on the machines so it helps with
# permissions when mounting volumes

# Copy over `Cargo.lock` into home directory (copied overriding at each pipeline Rust compilation)
COPY --chown=ubuntu Cargo.lock Cargo.lock
COPY --chown=ubuntu Cargo.toml lib/Cargo.toml

RUN mkdir -p lib
# The crates needed for building pipelines
COPY --chown=ubuntu crates/ lib/crates/
COPY --chown=ubuntu README.md lib/README.md
RUN mkdir -p lib/sql-to-dbsp-compiler/lib
# Copy over the rust code and sql-to-dbsp script
COPY --chown=ubuntu sql-to-dbsp-compiler/lib lib/sql-to-dbsp-compiler/lib
COPY --chown=ubuntu sql-to-dbsp-compiler/temp lib/sql-to-dbsp-compiler/temp
COPY --chown=ubuntu sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp lib/sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp

# Install cargo and rust for this non-root user
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.87.0
# The download URL for mold uses x86_64/aarch64 whereas dpkg --print-architecture says amd64/arm64
RUN arch=`dpkg --print-architecture | sed "s/arm64/aarch64/g" | sed "s/amd64/x86_64/g"`; \
  curl -LO https://github.com/rui314/mold/releases/download/v2.40.1/mold-2.40.1-$arch-linux.tar.gz \
  && tar -xzvf mold-2.40.1-$arch-linux.tar.gz \
  && mv mold-2.40.1-$arch-linux $HOME/mold \
  && rm mold-2.40.1-$arch-linux.tar.gz
ENV PATH="$PATH:/home/ubuntu/.cargo/bin:/home/ubuntu/mold/bin"
ENV RUSTFLAGS="-C link-arg=-fuse-ld=mold"

#
# Build phase to specifically customize for the feldera OSS container
#
FROM base AS release

# Copy over demos
RUN mkdir -p demos
COPY --chown=ubuntu demo/packaged/sql demos

COPY --chown=ubuntu build/pipeline-manager pipeline-manager
RUN mkdir -p lib/sql-to-dbsp-compiler/SQL-compiler/target
COPY --chown=ubuntu build/sql2dbsp-jar-with-dependencies.jar lib/sql-to-dbsp-compiler/SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar

# Run the precompile phase to speed up Rust compilations during deployment
RUN ./pipeline-manager --bind-address=0.0.0.0 --sql-compiler-home=$HOME/lib/sql-to-dbsp-compiler --compilation-cargo-lock-path=$HOME/Cargo.lock --dbsp-override-path=$HOME/lib --precompile
ENV BANNER_ADDR=localhost
ENTRYPOINT ["./pipeline-manager", "--bind-address=0.0.0.0", "--sql-compiler-home=/home/ubuntu/lib/sql-to-dbsp-compiler", "--compilation-cargo-lock-path=/home/ubuntu/Cargo.lock", "--dbsp-override-path=/home/ubuntu/lib", "--demos-dir", "/home/ubuntu/demos"]
