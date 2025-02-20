FROM ubuntu:24.04 AS base

## Add non-root user for startup of pg-embed with rust backend

ARG USERNAME=user

RUN useradd -m $USERNAME && usermod -aG sudo $USERNAME
RUN echo "user:pass" | chpasswd

# Set locale to fix pg-embed startup
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
   openssl \
   cmake \
   pkg-config \
   libssl-dev \
   libsasl2-dev \
   ca-certificates \
   gnupg \
   protobuf-compiler

## Setup Java and Python

RUN apt-get install -y \
   gcc clang libclang-dev python3-pip python3-plumbum \
   hub numactl openjdk-21-jre-headless maven

## Install nodejs (optional)
RUN mkdir -p /etc/apt/keyrings
RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | sudo gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
ENV NODE_MAJOR=20
RUN echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" | sudo tee /etc/apt/sources.list.d/nodesource.list
RUN apt update --fix-missing && apt install nodejs

## Install Redpanda rpk to enable preparing demos

RUN ARCH=`dpkg --print-architecture`; \
   curl -LO --output-dir $HOME https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-$ARCH.zip \
   && unzip $HOME/rpk-linux-$ARCH.zip -d /bin/ \
   && rpk version \
   && rm $HOME/rpk-linux-$ARCH.zip

## Install python dependencies for the demos

RUN pip3 install --break-system-packages gdown kafka-python-ng

## Switch to non-root user

USER $USERNAME
ENV HOME /home/$USERNAME

## Install rustup and common components

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="$HOME/.cargo/bin:$PATH"

## Install Bun.js
RUN curl -fsSL https://bun.sh/install | bash -s "bun-v1.2.2"
ENV PATH="$HOME/.bun/bin:$PATH"
RUN $HOME/.bun/bin/bun install --global @hey-api/openapi-ts

RUN \
   rustup install 1.82.0 && \
   rustup component add rustfmt && \
   rustup component add clippy && \
   cargo install cargo-make

ENV PATH="$HOME/.local/bin:$PATH"
