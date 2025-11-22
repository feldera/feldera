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
  # rdkafka dependency needs libsasl2-dev and a CXX compiler
  libsasl2-dev libzstd-dev libzstd1 build-essential \
  # To install rust
  curl  \
  # For running the SQL compiler
  openjdk-21-jre-headless -y \
  # Install locale-gen
  locales \
  # To add the nodesource debian repository
  ca-certificates gnupg \
  # Otherwise, postgresql_embedded complains when it tries to set timezeone to UTC
  tzdata \
  # Used by pipeline manager to switch runtime versions
  git

# Add AWS RDS bundle to trusted certificate store
RUN curl -sS https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem \
      -o /usr/local/share/ca-certificates/aws-global-rds-bundle.crt
RUN update-ca-certificates

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

RUN mkdir -p feldera
COPY --chown=ubuntu Cargo.lock feldera/Cargo.lock
COPY --chown=ubuntu Cargo.toml feldera/Cargo.toml
COPY --chown=ubuntu build/ feldera/build
COPY --chown=ubuntu crates/ feldera/crates
COPY --chown=ubuntu README.md feldera/README.md
RUN mkdir -p feldera/sql-to-dbsp-compiler/lib
# Copy over the rust code and sql-to-dbsp script
COPY --chown=ubuntu sql-to-dbsp-compiler/lib feldera/sql-to-dbsp-compiler/lib
COPY --chown=ubuntu sql-to-dbsp-compiler/temp feldera/sql-to-dbsp-compiler/temp
COPY --chown=ubuntu sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp feldera/sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp
# Copy over demos
RUN mkdir -p feldera/demo/packaged/sql
COPY --chown=ubuntu demo/packaged/sql feldera/demo/packaged/sql

# Install cargo and rust for this non-root user
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.87.0
# The download URL for mold uses x86_64/aarch64 whereas dpkg --print-architecture says amd64/arm64
RUN arch=`dpkg --print-architecture | sed "s/arm64/aarch64/g" | sed "s/amd64/x86_64/g"`; \
  curl -LO https://github.com/rui314/mold/releases/download/v2.40.1/mold-2.40.1-$arch-linux.tar.gz \
  && tar -xzvf mold-2.40.1-$arch-linux.tar.gz \
  && mv mold-2.40.1-$arch-linux $HOME/mold \
  && rm mold-2.40.1-$arch-linux.tar.gz
ENV PATH="$PATH:/home/ubuntu/.cargo/bin:/home/ubuntu/mold/bin"
ENV RUSTFLAGS="-C link-arg=-fuse-ld=mold -C link-arg=-Wl,--compress-debug-sections=zlib"

#
# Build phase to specifically customize for the feldera OSS container
#
FROM base AS release

# Run the precompile phase to speed up Rust compilations during deployment
RUN ./feldera/build/pipeline-manager --bind-address=0.0.0.0 --sql-compiler-path=$HOME/feldera/build/sql2dbsp-jar-with-dependencies.jar --compilation-cargo-lock-path=$HOME/feldera/Cargo.lock --dbsp-override-path=$HOME/feldera --precompile
ENV BANNER_ADDR=localhost
ENTRYPOINT ["./feldera/build/pipeline-manager", "--bind-address=0.0.0.0", "--sql-compiler-path=/home/ubuntu/feldera/build/sql2dbsp-jar-with-dependencies.jar", "--compilation-cargo-lock-path=/home/ubuntu/feldera/Cargo.lock", "--dbsp-override-path=/home/ubuntu/feldera", "--demos-dir", "/home/ubuntu/feldera/demo/packaged/sql"]
