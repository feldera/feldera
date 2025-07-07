# This Dockerfile is used by CI to build all things.
#
# The image is built by the `build-docker-dev.yml` action
# You can run it manually using the github actions UI.
#
# You can test it locally using:
# `docker build -f deploy/build.Dockerfile -t feldera-dev .`

FROM ubuntu:24.04 AS ubuntu-base
ENV DEBIAN_FRONTEND=noninteractive

# These two environment variables are used to make openssl-sys pick
# up libssl-dev and statically link it. Without it, our build defaults
# to building a vendored version of OpenSSL.
FROM ubuntu-base AS install-pkgs
ENV OPENSSL_NO_VENDOR=1
ENV OPENSSL_STATIC=1
RUN apt-get update --fix-missing && apt-get install -y \
    # pkg-config is required for cargo to find libssl
    libssl-dev pkg-config \
    cmake \
    # rdkafka dependency needs libsasl2-dev and a CXX compiler
    libsasl2-dev build-essential \
    # bindgen needs this (at least the dec crate uses bindgen)
    libclang-dev \
    # To download tools
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
    # Required for bun/aws cli installation
    unzip \
    # For debugging things
    strace \
    # For blacksmith runners configuring disks
    sudo \
    # Looks like on our arm machines we need npm,
    # otherwise `bun run build` fails during `svelte-kit sync` with SIGABRT (?)
    # somehow it works fine without npm on x86_64 machines :/
    npm

FROM install-pkgs AS base
# Give ubuntu user with sudo privileges for mounting dirs in blacksmith runner
RUN usermod -aG sudo ubuntu
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# Install redpanda's rpk cli
RUN arch=`dpkg --print-architecture`; \
    curl -LO https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-$arch.zip \
    && unzip rpk-linux-$arch.zip -d /bin/ \
    && rpk version \
    && rm rpk-linux-$arch.zip

# Install aws cli
RUN cd /home/ubuntu && curl -sSL "https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m).zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf aws awscliv2.zip

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
ENV PATH="/home/ubuntu/.local/bin:/home/ubuntu/.bun/bin:/home/ubuntu/.cargo/bin:/home/ubuntu/mold/bin:/home/ubuntu/gradle-8.7/bin:$PATH"

# Install rust
ENV RUSTUP_HOME=/home/ubuntu/.rustup
ENV CARGO_HOME=/home/ubuntu/.cargo
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile default --default-toolchain 1.87.0
RUN cargo install cargo-machete@0.7.0 cargo-edit@0.13.1 just@1.40.0

# Install uv
RUN curl -LsSf https://astral.sh/uv/0.6.5/install.sh | sh
RUN uv python install 3.10
RUN uv tool install pre-commit --with pre-commit-uv --force-reinstall

# Install Bun.js
RUN curl -fsSL https://bun.sh/install | bash -s "bun-v1.2.2"

# Install gradle, the version needs to match the version that calcite uses which avoid reinstalling this every time we run build.sh
RUN cd /home/ubuntu \
    && curl -LO https://services.gradle.org/distributions/gradle-8.7-bin.zip \
    && unzip gradle-8.7-bin.zip \
    && rm gradle-8.7-bin.zip

# The download URL for mold uses x86_64/aarch64 whereas dpkg --print-architecture says amd64/arm64
RUN arch=`dpkg --print-architecture | sed "s/arm64/aarch64/g" | sed "s/amd64/x86_64/g"`; \
    cd /home/ubuntu && curl -LO https://github.com/rui314/mold/releases/download/v2.40.1/mold-2.40.1-$arch-linux.tar.gz \
    && tar -xzvf mold-2.40.1-$arch-linux.tar.gz \
    && mv mold-2.40.1-$arch-linux /home/ubuntu/mold \
    && rm mold-2.40.1-$arch-linux.tar.gz

# Install sccache
RUN  arch=`dpkg --print-architecture | sed "s/arm64/aarch64/g" | sed "s/amd64/x86_64/g"`; \
    cd /home/ubuntu && curl -LO https://github.com/mozilla/sccache/releases/download/v0.10.0/sccache-v0.10.0-$arch-unknown-linux-musl.tar.gz \
    && tar zxvf sccache-v0.10.0-$arch-unknown-linux-musl.tar.gz \
    && cp sccache-v0.10.0-$arch-unknown-linux-musl/sccache /home/ubuntu/.cargo/bin \
    && chmod +x /home/ubuntu/.cargo/bin/sccache

ENV RUSTFLAGS="-C link-arg=-fuse-ld=mold"
RUN rustup default stable
