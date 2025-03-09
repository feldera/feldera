# This Dockerfile is used by CI to build things.
#
# To build the image, run:
#    docker build -f build.Dockerfile -t ghcr.io/gz/feldera-dev:latest .
#
# To inspect the image locally, run:
#    docker run -it ghcr.io/gz/feldera-dev:latest
#
# To push the image to GitHub Container Registry, run:
#    docker push ghcr.io/gz/feldera-dev:latest

# The build image contains tools to build the code given that
# we need a Java and Rust compiler to run alongside the pipeline manager
# as of now. This will change later.
FROM ubuntu:24.04 AS base
ENV DEBIAN_FRONTEND=noninteractive

# These two environment variables are used to make openssl-sys pick
# up libssl-dev and statically link it. Without it, our build defaults
# to building a vendored version of OpenSSL.
ENV OPENSSL_NO_VENDOR=1
ENV OPENSSL_STATIC=1
RUN apt-get update --fix-missing && apt-get install -y \
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
    protobuf-compiler \
    # Required for building the SQL compiler
    maven \
    # Required for building of many crates
    git \
    # Required for parsing json in some scripts
    jq \
    # Required for bun installation
    unzip \
    # For debugging things
    strace

# Install redpanda's rpk cli
RUN arch=`dpkg --print-architecture`; \
    curl -LO https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-$arch.zip \
    && unzip rpk-linux-$arch.zip -d /bin/ \
    && rpk version \
    && rm rpk-linux-$arch.zip

# Set UTF-8 locale. Needed for the Rust compiler to handle Unicode column names.
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
    locale-gen
ENV LC_ALL=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
# Making gcc, installed as a part of build-essentials, a default C compiler to build Rust dependencies
ENV CC=gcc-13

# We use the ubuntu user here since it has the same UID and GID (1000) as the ci user on the machines
# which helps with permissions when mounting volumes
USER ubuntu
ENV PATH="/home/ubuntu/.local/bin:/home/ubuntu/.bun/bin:/home/ubuntu/.cargo/bin:/home/ubuntu/mold/bin:$PATH"

# Install rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.83.0

## Install uv
RUN curl -LsSf https://astral.sh/uv/0.6.5/install.sh | sh
RUN uv python install 3.10

## Install Bun.js
RUN curl -fsSL https://bun.sh/install | bash -s "bun-v1.2.2"

# The download URL for mold uses x86_64/aarch64 whereas dpkg --print-architecture says amd64/arm64
RUN arch=`dpkg --print-architecture | sed "s/arm64/aarch64/g" | sed "s/amd64/x86_64/g"`; \
    cd /home/ubuntu && curl -LO https://github.com/rui314/mold/releases/download/v2.32.1/mold-2.32.1-$arch-linux.tar.gz \
    && tar -xzvf mold-2.32.1-$arch-linux.tar.gz \
    && mv mold-2.32.1-$arch-linux /home/ubuntu/mold \
    && rm mold-2.32.1-$arch-linux.tar.gz

ENV RUSTFLAGS="-C link-arg=-fuse-ld=mold"
RUN rustup default stable