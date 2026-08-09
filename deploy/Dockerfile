# librdkafka, built against AWS-LC rather than the system OpenSSL.
#
# This is a separate stage because building AWS-LC needs Go and cmake, and
# neither belongs in the shipped image. Only the installed library crosses over.
FROM ubuntu:24.04 AS librdkafka
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update --fix-missing && apt-get install -y \
  git cmake build-essential pkg-config golang-go \
  libsasl2-dev zlib1g-dev libzstd-dev
COPY scripts/install-librdkafka.sh /tmp/install-librdkafka.sh
# The script reads the librdkafka version from the lockfile, so the library and
# the crate expecting it cannot drift apart. A stale image fails loudly at the
# pkg-config probe rather than building against the wrong version.
COPY Cargo.lock /tmp/Cargo.lock
RUN CARGO_LOCK=/tmp/Cargo.lock PREFIX=/usr/local bash /tmp/install-librdkafka.sh

# The base image contains tools to build the code given that
# we need a Java and Rust compiler to run alongside the pipeline manager
# as of now. This will change later.
FROM ubuntu:24.04 AS base
ENV DEBIAN_FRONTEND=noninteractive

RUN apt update --fix-missing && apt -y dist-upgrade && apt install \
  # bindgen needs this (at least the dec crate uses bindgen)
  libclang-dev \
  # pkg-config is how rdkafka-sys locates the librdkafka copied in below
  pkg-config \
  cmake \
  # librdkafka links these; it is built in the stage above, but pipelines are
  # compiled in this image and the linker resolves them here
  libsasl2-dev zlib1g-dev libzstd-dev build-essential \
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
  git \
  # Used to set capabilities to the samply profiler
  libcap2-bin

# Add AWS RDS bundle to trusted certificate store
RUN curl -sS https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem \
      -o /usr/local/share/ca-certificates/aws-global-rds-bundle.crt
RUN update-ca-certificates

# Set UTF-8 locale. Needed for the Rust compiler to handle Unicode column names.
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
  locale-gen

# librdkafka from the stage above. rdkafka-sys links it dynamically, so it has
# to be here both to build pipelines and to run them.
COPY --from=librdkafka /usr/local/lib/librdkafka* /usr/local/lib/
COPY --from=librdkafka /usr/local/include/librdkafka /usr/local/include/librdkafka
COPY --from=librdkafka /usr/local/lib/pkgconfig/rdkafka*.pc /usr/local/lib/pkgconfig/
RUN ldconfig \
  # A NEEDED entry for libssl or libcrypto means librdkafka found a system
  # OpenSSL and preferred it over AWS-LC, which is the failure this whole
  # arrangement exists to prevent and is invisible at run time.
  && if readelf -d /usr/local/lib/librdkafka.so.1 | grep -qE 'Shared library: \[lib(ssl|crypto)\.so'; then \
       echo "error: librdkafka links a shared OpenSSL" >&2; exit 1; \
     fi \
  # rdkafka-sys probes for librdkafka this way; failing here beats failing
  # partway through a pipeline compilation.
  && pkg-config --exists rdkafka

USER ubuntu
WORKDIR /home/ubuntu

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
RUN mkdir -p feldera/demos/sql
COPY --chown=ubuntu crates/pipeline-manager/demos/sql feldera/demos/sql

# Install cargo and rust for this non-root user
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.93.1
# The download URL for mold uses x86_64/aarch64 whereas dpkg --print-architecture says amd64/arm64
RUN arch=`dpkg --print-architecture | sed "s/arm64/aarch64/g" | sed "s/amd64/x86_64/g"`; \
  curl -LO https://github.com/rui314/mold/releases/download/v2.40.4/mold-2.40.4-$arch-linux.tar.gz \
  && tar -xzvf mold-2.40.4-$arch-linux.tar.gz \
  && mv mold-2.40.4-$arch-linux $HOME/mold \
  && rm mold-2.40.4-$arch-linux.tar.gz
ENV PATH="$PATH:/home/ubuntu/.cargo/bin:/home/ubuntu/mold/bin"
ENV RUSTFLAGS="-C link-arg=-fuse-ld=mold -C link-arg=-Wl,--compress-debug-sections=zlib"

# Install Samply for profiling
RUN curl --proto '=https' --tlsv1.2 -LsSf https://github.com/feldera/samply/releases/download/v0.13.2/samply-installer.sh | sh

#
# Build phase to specifically customize for the feldera OSS container
#
FROM base AS release

# Run the precompile phase to speed up Rust compilations during deployment
RUN ./feldera/build/pipeline-manager --bind-address=0.0.0.0 --sql-compiler-path=$HOME/feldera/build/sql2dbsp-jar-with-dependencies.jar --compilation-cargo-lock-path=$HOME/feldera/Cargo.lock --dbsp-override-path=$HOME/feldera --precompile
ENV BANNER_ADDR=localhost
ENTRYPOINT ["./feldera/build/pipeline-manager", "--bind-address=0.0.0.0", "--sql-compiler-path=/home/ubuntu/feldera/build/sql2dbsp-jar-with-dependencies.jar", "--compilation-cargo-lock-path=/home/ubuntu/feldera/Cargo.lock", "--dbsp-override-path=/home/ubuntu/feldera", "--demos-dir", "/home/ubuntu/feldera/demos/sql"]
