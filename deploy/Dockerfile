# The base image contains tools to build the code given that
# we need a Java and Rust compiler to run alongside the pipeline manager
# as of now. This will change later.
FROM ubuntu:24.04 AS base
ENV DEBIAN_FRONTEND noninteractive
# These two environment variables are used to make openssl-sys pick
# up libssl-dev and statically link it. Without it, our build defaults
# to building a vendored version of OpenSSL.
ENV OPENSSL_NO_VENDOR=1
ENV OPENSSL_STATIC=1
RUN apt update --fix-missing && apt install \
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
  ca-certificates gnupg

# Set UTF-8 locale. Needed for the Rust compiler to handle Unicode column names.
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
  locale-gen
ENV LC_ALL en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en

# Use cargo-chef to produce a recipe.json file
# to cache the requisite dependencies
FROM base as chef
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
RUN /root/.cargo/bin/cargo install cargo-chef
WORKDIR app

# Build web-ui
FROM base as web-ui-builder
# - nodejs
RUN mkdir -p /etc/apt/keyrings
RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
ENV NODE_MAJOR=20
RUN echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list
RUN apt update --fix-missing && apt install nodejs -y
RUN npm install --global yarn openapi-typescript-codegen
# - yarn
COPY web-console/package.json ./web-console/package.json
COPY web-console/yarn.lock ./web-console/yarn.lock
RUN cd web-console && yarn install
# Use a glob here so that the .env file's presence is optional. This should
# not fail in our CI/CD builds and if it does, we should see a MUIX license
# error in the UI
COPY web-console/.env* web-console/
COPY web-console/public web-console/public
COPY web-console/src web-console/src
COPY web-console/.editorconfig web-console/
COPY web-console/.eslintrc.json web-console/
COPY web-console/.prettierrc.js web-console/
COPY web-console/next-env.d.ts ./web-console/next-env.d.ts
COPY web-console/next.config.js ./web-console/next.config.js
COPY web-console/next.d.ts ./web-console/next.d.ts
COPY web-console/tsconfig.json ./web-console/tsconfig.json
RUN cd web-console && yarn build

# Cache dependencies from rust
FROM chef AS planner
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY crates crates
RUN mkdir sql-to-dbsp-compiler
COPY sql-to-dbsp-compiler/lib sql-to-dbsp-compiler/lib
RUN /root/.cargo/bin/cargo chef prepare --recipe-path recipe.json

# Use the recipe.json file to build dependencies first and cache that
# layer for faster incremental builds of source-code only changes
FROM chef AS builder
ENV CARGO_INCREMENTAL=0
COPY --from=planner /app/recipe.json recipe.json
RUN /root/.cargo/bin/cargo chef cook --release --recipe-path recipe.json --bin=pipeline-manager --features=pg-embed
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY crates crates
RUN mkdir sql-to-dbsp-compiler || true
COPY sql-to-dbsp-compiler/lib sql-to-dbsp-compiler/lib
COPY --from=web-ui-builder /web-console/out web-console-out
ENV WEBUI_BUILD_DIR=/app/web-console-out
RUN /root/.cargo/bin/cargo build --release --bin=pipeline-manager --features=pg-embed

# Java build can be performed in parallel
FROM base as javabuild
RUN apt install maven -y
RUN mkdir sql
COPY sql-to-dbsp-compiler /sql/sql-to-dbsp-compiler
RUN --mount=type=cache,target=/root/.m2 \
    cd /sql/sql-to-dbsp-compiler/SQL-compiler && mvn -ntp -DskipTests package

# Minimal image for running the pipeline manager
FROM base as release
# Pipeline manager binary
RUN useradd -ms /bin/bash feldera
USER feldera
WORKDIR /home/feldera
COPY --from=builder /app/target/release/pipeline-manager pipeline-manager
# SQL compiler uber jar
RUN mkdir -p lib/sql-to-dbsp-compiler/SQL-compiler/target
COPY --from=javabuild /sql/sql-to-dbsp-compiler/SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar lib/sql-to-dbsp-compiler/SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar
# The crates needed for the SQL compiler
COPY crates/dbsp lib/crates/dbsp
COPY crates/pipeline-types lib/crates/pipeline-types
COPY crates/adapters lib/crates/adapters
# Storage crate got folded into the dbsp crate for now. Revert if this changes.
# COPY crates/feldera-storage lib/crates/feldera-storage
COPY README.md lib/README.md
RUN mkdir -p lib/sql-to-dbsp-compiler/lib

# Copy over the rust code and sql-to-dbsp script
COPY sql-to-dbsp-compiler/lib lib/sql-to-dbsp-compiler/lib
COPY sql-to-dbsp-compiler/temp lib/sql-to-dbsp-compiler/temp
COPY sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp lib/sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp

# Install cargo and rust for this non-root user
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
ENV PATH="$PATH:/home/feldera/.cargo/bin"
# Run the precompile phase to speed up Rust compilations during deployment
RUN ./pipeline-manager --bind-address=0.0.0.0 --sql-compiler-home=/home/feldera/lib/sql-to-dbsp-compiler --dbsp-override-path=/home/feldera/lib --precompile
ENV BANNER_ADDR localhost
ENTRYPOINT ["./pipeline-manager", "--bind-address=0.0.0.0", "--sql-compiler-home=/home/feldera/lib/sql-to-dbsp-compiler", "--dbsp-override-path=/home/feldera/lib", "--allowed-origins", "https://www.feldera.com", "--allowed-origins", "http://localhost:8080"]

##### The stages below are used to build the demo container

# Prepare SecOps simulator recipe
FROM chef as demo-planner
COPY ./demo/project_demo00-SecOps/simulator/ .
RUN /root/.cargo/bin/cargo chef prepare --recipe-path recipe.json

# Use the recipe.json file to build dependencies first and cache that
# layer for faster incremental builds of source-code only changes
FROM chef AS demo-builder
COPY --from=demo-planner /app/recipe.json recipe.json
RUN /root/.cargo/bin/cargo chef cook --release --recipe-path recipe.json
COPY ./demo/project_demo00-SecOps/simulator/ .
RUN /root/.cargo/bin/cargo build --release

# The dev target adds an rpk client and demo projects
FROM ubuntu:24.04 AS client
ENV DEBIAN_FRONTEND noninteractive
ENV PATH="$PATH:/root/.cargo/bin"
COPY demo demo
# Remove the simulator cargo project and the corresponding build artifacts
RUN rm -rf ./demo/project_demo00-SecOps/simulator/*
COPY --from=demo-builder /app/target/release/secops_simulator demo/project_demo00-SecOps/simulator/
RUN apt update && apt install pkg-config \
  python3-pip python3-plumbum \
  curl unzip -y --no-install-recommends \
  # Install RPK
  && arch=`dpkg --print-architecture`; \
  curl -LO https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-$arch.zip \
  && unzip rpk-linux-$arch.zip -d /bin/ \
  && rpk version \
  && rm rpk-linux-$arch.zip \
  # Install snowsql
  && curl -O https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/1.2/linux_x86_64/snowsql-1.2.28-linux_x86_64.bash \
  && SNOWSQL_DEST=/bin SNOWSQL_LOGIN_SHELL=~/.profile bash snowsql-1.2.28-linux_x86_64.bash \
  && pip3 install --break-system-packages snowflake-connector-python \
  # TODO: only required for running the fraud detection demo. Remove when we clean that up.
  && pip3 install --break-system-packages gdown \
  && pip3 install --break-system-packages "psycopg[binary]" \
  && pip3 install --break-system-packages kafka-python \
  # cleanup packages we don't need anymore
  && apt remove python3-pip unzip pkg-config -y && apt autoremove -y
CMD bash

# Kafka connect with all Debezium connectors + the Snowflake connector.
FROM debezium/connect:2.5 AS kafka-connect
RUN mkdir /kafka/connect/snowflake-kafka-connector
RUN cd /kafka/connect/snowflake-kafka-connector \
  && curl -LO https://repo1.maven.org/maven2/com/snowflake/snowflake-kafka-connector/2.1.0/snowflake-kafka-connector-2.1.0.jar \
  && curl -LO https://repo1.maven.org/maven2/org/bouncycastle/bc-fips/1.0.1/bc-fips-1.0.1.jar \
  && curl -LO https://repo1.maven.org/maven2/org/bouncycastle/bcpkix-fips/1.0.3/bcpkix-fips-1.0.3.jar

# By default, only build the release version
FROM release
