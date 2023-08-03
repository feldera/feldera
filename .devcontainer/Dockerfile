FROM ubuntu:22.04 AS base

## Add non-root user for startup of pg-embed with rust backend

ARG USERNAME=user

RUN useradd -m $USERNAME && usermod -aG sudo $USERNAME
RUN echo "user:pass" | chpasswd

## Install common dependencies

RUN apt-get update && \
   apt-get install -y \
   curl \
   git \
   gnupg2 \
   sudo \
   build-essential \
   openssl \
   cmake \
   pkg-config \
   libssl-dev

## Setup Java and Python

RUN apt-get install -y \
   gcc clang libclang-dev python3-pip python3-plumbum \
   hub numactl openjdk-19-jre-headless maven

## Install nodejs and global packages

RUN curl -fsSL https://deb.nodesource.com/setup_19.x | bash -
RUN apt-get install -y nodejs
RUN npm install --global yarn
RUN npm install --global openapi-typescript-codegen

# Switch to non-root user
USER $USERNAME

## Install rustup and common components

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:/root/.cargo/env:${PATH}"

SHELL ["/bin/bash", "-c"]
RUN source "$HOME/.cargo/env" && \
   rustup install nightly && \
   rustup component add rustfmt && \
   rustup component add rustfmt --toolchain nightly && \
   rustup component add clippy && \
   rustup component add clippy --toolchain nightly
SHELL ["/bin/sh", "-c"]

# Set locale to fix pg-embed startup
ENV LC_ALL=en_US.UTF-8
