# The base image  contains tools to build the code given that
# we need a Java and Rust compiler to run alongside the pipeline manager
# as of now. This will change later.
FROM ubuntu:22.04 AS base
ENV DEBIAN_FRONTEND noninteractive
RUN apt update && apt install libssl-dev build-essential pkg-config \
     git gcc clang libclang-dev python3-pip python3-plumbum hub numactl cmake \
     curl openjdk-19-jre-headless maven netcat jq \
     adduser libfontconfig1 unzip -y
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
# web-ui build tools
RUN curl -fsSL https://deb.nodesource.com/setup_19.x | bash -
RUN apt-get install --yes nodejs
RUN npm install --global yarn
RUN npm install --global openapi-typescript-codegen

# Use cargo-chef to produce a recipe.json file
# to cache the requisite dependencies
FROM base as chef
RUN /root/.cargo/bin/cargo install cargo-chef
RUN /root/.cargo/bin/cargo install cargo-make
WORKDIR app

# Cache dependencies from rust
FROM chef AS planner
COPY . .
RUN /root/.cargo/bin/cargo chef prepare --recipe-path recipe.json

# Use the recipe.json file to build dependencies first and cache that
# layer for faster incremental builds of source-code only changes
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN /root/.cargo/bin/cargo chef cook --release --recipe-path recipe.json --bin=dbsp_pipeline_manager --no-default-features
COPY . .
RUN /root/.cargo/bin/cargo build --release --bin=dbsp_pipeline_manager --no-default-features

# Java build can be performed in parallel
FROM base as javabuild
RUN mkdir sql
COPY .git /sql/.git
COPY sql-to-dbsp-compiler /sql/sql-to-dbsp-compiler
RUN cd /sql/sql-to-dbsp-compiler/SQL-compiler && mvn -DskipTests package

# Minimal image for running the pipeline manager
FROM base as release
ENV PATH="$PATH:/root/.cargo/bin"
COPY --from=builder /app/target/release/dbsp_pipeline_manager dbsp_pipeline_manager
COPY --from=javabuild /sql/sql-to-dbsp-compiler sql-to-dbsp-compiler
COPY . /database-stream-processor
CMD ./dbsp_pipeline_manager --bind-address=0.0.0.0 --working-directory=/working-dir --sql-compiler-home=/sql-to-dbsp-compiler --dbsp-override-path=/database-stream-processor

# The dev target adds an rpk client and demo projects
FROM builder as client
ENV PATH="$PATH:/root/.cargo/bin"
RUN arch=`dpkg --print-architecture`; \
   curl -LO https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-$arch.zip \
   && unzip rpk-linux-$arch.zip -d /bin/ \
   && rpk version \
   && rm rpk-linux-$arch.zip
RUN cargo make --cwd crates/pipeline_manager openapi_python
# TODO: only required for running the fraud detection demo. Remove when we clean that up.
RUN pip3 install gdown
CMD bash

# By default, only build the release version
FROM release