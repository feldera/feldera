# This Dockerfile is used by CI to build all things.
#
# The image is built by the `build-docker-dev.yml` action
# You can run it manually using the github actions UI.
#
# You can test it locally using:
# `docker build -f deploy/build.Dockerfile -t feldera-dev .`

FROM ubuntu:24.04 AS ubuntu-base
ENV DEBIAN_FRONTEND=noninteractive

FROM ubuntu-base AS install-pkgs
RUN apt-get update --fix-missing && apt-get install -y \
    # pkg-config is how rdkafka-sys locates librdkafka
    pkg-config \
    cmake \
    # Go builds AWS-LC, which librdkafka is linked against; see
    # scripts/install-librdkafka.sh
    golang-go \
    # librdkafka links these
    libsasl2-dev libzstd-dev zlib1g-dev build-essential \
    # zstd CLI: @actions/cache (runs-on/cache) auto-uses it for cache
    # compression when on PATH, else falls back to slow gzip. Big win for the
    # multi-GB target/ caches in test-java.yml.
    zstd \
    # bindgen needs this (at least the dec crate uses bindgen)
    libclang-dev \
    # To download tools
    curl  \
    # For running the SQL compiler
    openjdk-21-jdk-headless -y \
    # Install locale-gen
    locales \
    # To add the nodesource debian repository
    ca-certificates gnupg \
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
    npm \
    # For envsubst (used in some scripts)
    gettext \
    # For testing the NATS input connector
    nats-server

# Install trufflehog
RUN curl -sSfL https://raw.githubusercontent.com/trufflesecurity/trufflehog/main/scripts/install.sh | sh -s -- -b /usr/local/bin v3.97.0

# Install docker cli tool
RUN  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
        | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null \
    && apt-get update \
    && apt-get install -y docker-ce-cli

## Install nodejs
RUN mkdir -p /etc/apt/keyrings
RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | sudo gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
ENV NODE_MAJOR=24
RUN echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" | sudo tee /etc/apt/sources.list.d/nodesource.list
RUN apt-get update --fix-missing && apt-get install -y nodejs

# Install helm cli tool
RUN curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 && chmod +x get_helm.sh \
    && ./get_helm.sh --version v3.21.4 \
    && rm ./get_helm.sh

# Install kubectl. Stay within one minor version of the EKS clusters (1.34),
# per the kubectl version-skew policy.
RUN curl -LO "https://dl.k8s.io/release/v1.34.10/bin/linux/$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')/kubectl" \
    && chmod +x kubectl \
    && mv kubectl /usr/local/bin/

# Install k3d
RUN curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | TAG=v5.9.0 bash

# Install the GitHub CLI
RUN arch=`dpkg --print-architecture`; \
    curl -LO https://github.com/cli/cli/releases/download/v2.97.0/gh_2.97.0_linux_$arch.tar.gz \
    && tar -xzf gh_2.97.0_linux_$arch.tar.gz \
    && mv gh_2.97.0_linux_$arch/bin/gh /usr/local/bin/ \
    && rm -rf gh_2.97.0_linux_$arch gh_2.97.0_linux_$arch.tar.gz

# Install actionlint (GitHub Actions workflow linter)
RUN arch=`dpkg --print-architecture`; \
    curl -LO https://github.com/rhysd/actionlint/releases/download/v1.7.12/actionlint_1.7.12_linux_$arch.tar.gz \
    && tar -xzf actionlint_1.7.12_linux_$arch.tar.gz actionlint \
    && mv actionlint /usr/local/bin/ \
    && rm actionlint_1.7.12_linux_$arch.tar.gz

# terraform, tflint, and trivy for pre-commit hooks
RUN arch=`dpkg --print-architecture`; \
    curl -LO https://releases.hashicorp.com/terraform/1.15.8/terraform_1.15.8_linux_$arch.zip \
    && unzip terraform_1.15.8_linux_$arch.zip terraform -d /usr/local/bin/ \
    && rm terraform_1.15.8_linux_$arch.zip
RUN arch=`dpkg --print-architecture`; \
    curl -LO https://github.com/terraform-linters/tflint/releases/download/v0.64.0/tflint_linux_$arch.zip \
    && unzip tflint_linux_$arch.zip -d /usr/local/bin/ \
    && rm tflint_linux_$arch.zip
RUN curl -sSfL https://raw.githubusercontent.com/aquasecurity/trivy/main/contrib/install.sh | sh -s -- -b /usr/local/bin v0.74.0

# dnscontrol deploys DNS in the infrastructure repo
RUN arch=`dpkg --print-architecture`; \
    curl -LO https://github.com/StackExchange/dnscontrol/releases/download/v4.46.0/dnscontrol_4.46.0_linux_$arch.tar.gz \
    && tar -xzf dnscontrol_4.46.0_linux_$arch.tar.gz dnscontrol \
    && mv dnscontrol /usr/local/bin/ \
    && rm dnscontrol_4.46.0_linux_$arch.tar.gz

FROM install-pkgs AS base

# Modify ubuntu user UID/GID to 1001 and create docker group with GID 123
# This corresponds to the permission of the default github runner and
# causes the least CI woes when used in docker/k8s
RUN groupmod -g 1001 ubuntu && usermod -u 1001 -g 1001 ubuntu \
    && groupadd -g 123 docker && usermod -aG docker,sudo ubuntu \
    && chown -R ubuntu:ubuntu /home/ubuntu \
    && echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

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

ENV GRADLE_VERSION=8.14.4

# We use the ubuntu user here since it has the same UID and GID (1000) as the ci user on the machines
# which helps with permissions when mounting volumes
USER ubuntu
ENV PATH="/home/ubuntu/.local/bin:/home/ubuntu/.bun/bin:/home/ubuntu/.cargo/bin:/home/ubuntu/mold/bin:/home/ubuntu/gradle-${GRADLE_VERSION}/bin:$PATH"

# Install rust
ENV RUSTUP_HOME=/home/ubuntu/.rustup
ENV CARGO_HOME=/home/ubuntu/.cargo
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile default --default-toolchain 1.93.1
RUN cargo install --locked cargo-machete@0.9.2 cargo-edit@0.13.13 just@1.58.0

# Install uv
RUN curl -LsSf https://astral.sh/uv/0.12.5/install.sh | sh
RUN uv python install 3.10
RUN uv tool install pre-commit --with pre-commit-uv --force-reinstall

# Install Bun.js
RUN curl -fsSL https://bun.sh/install | bash -s "bun-v1.3.14"

# Install gradle, the version needs to match the version that calcite uses which avoid reinstalling this every time we run build.sh
RUN cd /home/ubuntu \
    && curl -LO https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip \
    && unzip gradle-${GRADLE_VERSION}-bin.zip \
    && rm gradle-${GRADLE_VERSION}-bin.zip

# The download URL for mold uses x86_64/aarch64 whereas dpkg --print-architecture says amd64/arm64
RUN arch=`dpkg --print-architecture | sed "s/arm64/aarch64/g" | sed "s/amd64/x86_64/g"`; \
    cd /home/ubuntu && curl -LO https://github.com/rui314/mold/releases/download/v2.42.0/mold-2.42.0-$arch-linux.tar.gz \
    && tar -xzvf mold-2.42.0-$arch-linux.tar.gz \
    && mv mold-2.42.0-$arch-linux /home/ubuntu/mold \
    && rm mold-2.42.0-$arch-linux.tar.gz

# Install sccache
RUN  arch=`dpkg --print-architecture | sed "s/arm64/aarch64/g" | sed "s/amd64/x86_64/g"`; \
    cd /home/ubuntu && curl -LO https://github.com/mozilla/sccache/releases/download/v0.17.0/sccache-v0.17.0-$arch-unknown-linux-musl.tar.gz \
    && tar zxvf sccache-v0.17.0-$arch-unknown-linux-musl.tar.gz \
    && cp sccache-v0.17.0-$arch-unknown-linux-musl/sccache /home/ubuntu/.cargo/bin \
    && chmod +x /home/ubuntu/.cargo/bin/sccache \
    && rm -rf sccache-v0.17.0-$arch-unknown-linux-musl.tar.gz sccache-v0.17.0-$arch-unknown-linux-musl

ENV RUSTFLAGS="-C link-arg=-fuse-ld=mold -C link-arg=-Wl,--compress-debug-sections=zlib"

# librdkafka, built against AWS-LC rather than the system OpenSSL. rdkafka is
# built with `dynamic-linking`, so it consumes this rather than compiling its
# own copy.
#
# Last in the file on purpose: it copies Cargo.lock, from which the script
# reads the librdkafka version the crate expects, and that file changes often.
# Keeping it here means a lockfile change rebuilds only this layer.
USER root
COPY scripts/install-librdkafka.sh /tmp/install-librdkafka.sh
COPY Cargo.lock /tmp/Cargo.lock
RUN CARGO_LOCK=/tmp/Cargo.lock PREFIX=/usr/local bash /tmp/install-librdkafka.sh \
    && rm -f /tmp/install-librdkafka.sh /tmp/Cargo.lock
USER ubuntu
