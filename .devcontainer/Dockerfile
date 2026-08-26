FROM ubuntu:24.04 AS base

## Add non-root user for startup of postgres with rust backend

ARG USERNAME=user

# Rename the default user (uid 1000) from `ubuntu` to USERNAME
RUN usermod -l $USERNAME -d /home/$USERNAME -m ubuntu
ENV HOME /home/$USERNAME
RUN echo "$USERNAME:pass" | chpasswd

# Set locale to fix postgres embedded startup
ENV LC_ALL=en_US.UTF-8

## Install common dependencies

RUN apt-get update --fix-missing && \
   apt-get install -y \
   curl \
   unzip \
   git \
   gnupg2 \
   sudo \
   build-essential \
   # Go builds AWS-LC in scripts/install-librdkafka.sh, and aws-lc-fips-sys
   # when rustls is built with FIPS
   golang-go \
   openssl \
   cmake \
   pkg-config \
   libssl-dev \
   # librdkafka links these; scripts/install-librdkafka.sh needs their headers
   # to enable SASL, zlib and zstd support
   libsasl2-dev \
   zlib1g-dev \
   libzstd-dev \
   ca-certificates \
   gnupg \
   protobuf-compiler \
   git \
   jq

# postCreate.sh installs certain packages as root from a shell with no terminal to type the
# password set above into. sudo owns /etc/sudoers.d, so this has to follow the
# install. visudo -c fails the build on a syntax error here, which would
# otherwise leave sudo refusing to run at all, and only at postCreate time.
RUN echo "$USERNAME ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/$USERNAME && \
   chmod 0440 /etc/sudoers.d/$USERNAME && \
   visudo -cf /etc/sudoers.d/$USERNAME

## Setup Java and Python

RUN apt-get install --no-install-recommends -y \
   gcc clang libclang-dev python3-pip python3-plumbum \
   hub numactl openjdk-21-jdk-headless maven
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

## Install nodejs
RUN mkdir -p /etc/apt/keyrings
RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | sudo gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
ENV NODE_MAJOR=24
RUN echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" | sudo tee /etc/apt/sources.list.d/nodesource.list
RUN apt update --fix-missing && apt install -y nodejs

## Install Redpanda rpk to enable preparing demos

RUN ARCH=`dpkg --print-architecture`; \
   curl -LO --output-dir $HOME https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-$ARCH.zip \
   && unzip $HOME/rpk-linux-$ARCH.zip -d /bin/ \
   && rpk version \
   && rm $HOME/rpk-linux-$ARCH.zip

## Install python dependencies for the demos

RUN pip3 install --break-system-packages gdown kafka-python-ng

## Prepare directories for non-root user (for Claude Code)
# The installers above ran as root with HOME set to this directory, so they left
# root-owned dotfiles behind: uv writes ~/.config and ~/.zshrc. Hand the whole
# home directory over, or anything the user later runs that wants a new dotfile
# fails, as the samply installer does on ~/.config/samply.
RUN mkdir -p /home/$USERNAME/.local/share/claude \
   /home/$USERNAME/.local/state/claude \
   /home/$USERNAME/.local/bin && \
   chown -R $USERNAME /home/$USERNAME

## Switch to non-root user

USER $USERNAME

## Install rustup and common components
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="$HOME/.cargo/bin:$PATH"

## Install Bun.js
RUN curl -fsSL https://bun.sh/install | bash -s "bun-v1.3.3"
ENV PATH="$HOME/.bun/bin:$PATH"
RUN $HOME/.bun/bin/bun install --global @hey-api/openapi-ts @biomejs/biome

## Install Claude Code
RUN curl -fsSL https://claude.ai/install.sh | bash

RUN \
   rustup install $RUST_VERSION && \
   rustup default $RUST_VERSION && \
   rustup component add rustfmt && \
   rustup component add clippy && \
   cargo install cargo-make

## Install Samply
RUN curl --proto '=https' --tlsv1.2 -LsSf https://github.com/feldera/samply/releases/download/v0.13.2/samply-installer.sh | sh

ENV PATH="$HOME/.local/bin:$PATH"
