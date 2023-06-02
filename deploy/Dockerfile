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
COPY sql-to-dbsp-compiler /sql/sql-to-dbsp-compiler
RUN cd /sql/sql-to-dbsp-compiler/SQL-compiler && mvn -DskipTests package

# Minimal image for running the pipeline manager
FROM base as release
ENV PATH="$PATH:/root/.cargo/bin"
# Pipeline manager binary
COPY --from=builder /app/target/release/dbsp_pipeline_manager dbsp_pipeline_manager
# SQL compiler uber jar
RUN mkdir -p database-stream-processor/sql-to-dbsp-compiler/SQL-compiler/target
COPY --from=javabuild /sql/sql-to-dbsp-compiler/SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar database-stream-processor/sql-to-dbsp-compiler/SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar
# The crates needed for the SQL compiler
COPY crates/dbsp database-stream-processor/crates/dbsp
COPY crates/adapters database-stream-processor/crates/adapters
COPY crates/dataflow-jit database-stream-processor/crates/dataflow-jit
COPY README.md database-stream-processor/README.md
RUN mkdir -p database-stream-processor/sql-to-dbsp-compiler/lib

# Copy over the rust code and sql-to-dbsp script
COPY sql-to-dbsp-compiler/lib database-stream-processor/sql-to-dbsp-compiler/lib
COPY sql-to-dbsp-compiler/temp database-stream-processor/sql-to-dbsp-compiler/temp
COPY sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp database-stream-processor/sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp

CMD ./dbsp_pipeline_manager --bind-address=0.0.0.0 --working-directory=/working-dir --sql-compiler-home=/database-stream-processor/sql-to-dbsp-compiler --dbsp-override-path=/database-stream-processor

# The dev target adds an rpk client and demo projects
FROM ubuntu:22.04 AS client
ENV DEBIAN_FRONTEND noninteractive
RUN apt update && apt install libssl-dev build-essential pkg-config \
     git gcc clang libclang-dev python3-pip python3-plumbum hub numactl cmake \
     curl unzip -y
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="$PATH:/root/.cargo/bin"
RUN arch=`dpkg --print-architecture`; \
   curl -LO https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-$arch.zip \
   && unzip rpk-linux-$arch.zip -d /bin/ \
   && rpk version \
   && rm rpk-linux-$arch.zip
COPY --from=builder /app/target/release/dbsp_pipeline_manager dbsp_pipeline_manager
COPY demo demo
COPY python python
RUN ./dbsp_pipeline_manager --dump-openapi
RUN rm dbsp_pipeline_manager
RUN pip3 install openapi-python-client websockets
RUN cd python &&  \
    openapi-python-client generate --path ../openapi.json && \
    pip3 install ./dbsp-api-client && \ 
    pip3 install .
# TODO: only required for running the fraud detection demo. Remove when we clean that up.
RUN pip3 install gdown
CMD bash

# By default, only build the release version
FROM release