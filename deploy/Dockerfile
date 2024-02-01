# The base image contains tools to build the code given that
# we need a Java and Rust compiler to run alongside the pipeline manager
# as of now. This will change later.
FROM ubuntu:22.04 AS base
ENV DEBIAN_FRONTEND noninteractive
RUN apt update --fix-missing && apt install \
  # pkg-config is required for cargo to find libssl
  libssl-dev pkg-config \
  # rdkafka dependency needs cmake and a CXX compiler
  cmake build-essential \
  # To install rust
  curl  \
  # For running the SQL compiler
  openjdk-19-jre-headless -y \
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
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal

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
RUN /root/.cargo/bin/cargo chef cook --release --recipe-path recipe.json --package=pipeline-manager --no-default-features
COPY . .
# web-console build tools:
# - nodejs
RUN mkdir -p /etc/apt/keyrings
RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
ENV NODE_MAJOR=20
RUN echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list
RUN apt update --fix-missing && apt install nodejs -y
# - yarn
RUN npm install --global yarn openapi-typescript-codegen
RUN /root/.cargo/bin/cargo build --release --package=pipeline-manager --no-default-features

# Java build can be performed in parallel
FROM base as javabuild
RUN apt install maven -y
RUN mkdir sql
COPY sql-to-dbsp-compiler /sql/sql-to-dbsp-compiler
RUN cd /sql/sql-to-dbsp-compiler/SQL-compiler && mvn -ntp -DskipTests package

# Minimal image for running the pipeline manager
FROM base as release
ENV PATH="$PATH:/root/.cargo/bin"
# Pipeline manager binary
COPY --from=builder /app/target/release/pipeline-manager pipeline-manager
# SQL compiler uber jar
RUN mkdir -p database-stream-processor/sql-to-dbsp-compiler/SQL-compiler/target
COPY --from=javabuild /sql/sql-to-dbsp-compiler/SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar database-stream-processor/sql-to-dbsp-compiler/SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar
# The crates needed for the SQL compiler
COPY crates/dbsp database-stream-processor/crates/dbsp
COPY crates/pipeline-types database-stream-processor/crates/pipeline-types
COPY crates/adapters database-stream-processor/crates/adapters
COPY crates/feldera-storage database-stream-processor/crates/feldera-storage
COPY README.md database-stream-processor/README.md
RUN mkdir -p database-stream-processor/sql-to-dbsp-compiler/lib

# Copy over the rust code and sql-to-dbsp script
COPY sql-to-dbsp-compiler/lib database-stream-processor/sql-to-dbsp-compiler/lib
COPY sql-to-dbsp-compiler/temp database-stream-processor/sql-to-dbsp-compiler/temp
COPY sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp database-stream-processor/sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp
# Run the precompile phase to speed up Rust compilations during deployment
RUN ./pipeline-manager --bind-address=0.0.0.0 --api-server-working-directory=/working-dir --compiler-working-directory=/working-dir --sql-compiler-home=/database-stream-processor/sql-to-dbsp-compiler --dbsp-override-path=/database-stream-processor --precompile
ENV BANNER_ADDR localhost
ENTRYPOINT ["./pipeline-manager", "--bind-address=0.0.0.0", "--api-server-working-directory=/working-dir", "--compiler-working-directory=/working-dir", "--runner-working-directory=/working-dir", "--sql-compiler-home=/database-stream-processor/sql-to-dbsp-compiler", "--dbsp-override-path=/database-stream-processor", "--allowed-origins", "https://www.feldera.com", "--allowed-origins", "http://localhost:8080"]

# Standalone api-server
FROM gcr.io/distroless/cc-debian12 AS api-server
COPY --from=builder /app/target/release/api-server api-server
ENV BANNER_ADDR localhost
ENTRYPOINT ["./api-server", "--bind-address=0.0.0.0", "--api-server-working-directory=/working-dir"]

# Standalone compiler-server
FROM base as compiler-server
ENV PATH="$PATH:/root/.cargo/bin"
COPY --from=builder /app/target/release/compiler-server compiler-server
# SQL compiler uber jar
RUN mkdir -p database-stream-processor/sql-to-dbsp-compiler/SQL-compiler/target
COPY --from=javabuild /sql/sql-to-dbsp-compiler/SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar database-stream-processor/sql-to-dbsp-compiler/SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar
# The crates needed for the SQL compiler
COPY crates/dbsp database-stream-processor/crates/dbsp
COPY crates/pipeline-types database-stream-processor/crates/pipeline-types
COPY crates/adapters database-stream-processor/crates/adapters
COPY crates/feldera-storage database-stream-processor/crates/feldera-storage
COPY README.md database-stream-processor/README.md
RUN mkdir -p database-stream-processor/sql-to-dbsp-compiler/lib
# Copy over the rust code and sql-to-dbsp script
COPY sql-to-dbsp-compiler/lib database-stream-processor/sql-to-dbsp-compiler/lib
COPY sql-to-dbsp-compiler/temp database-stream-processor/sql-to-dbsp-compiler/temp
COPY sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp database-stream-processor/sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp
# Run the precompile phase to speed up Rust compilations during deployment
# TODO: disabling this to keep compiler image sizes small
# RUN ./compiler-server --compiler-working-directory=/working-dir --sql-compiler-home=/database-stream-processor/sql-to-dbsp-compiler --dbsp-override-path=/database-stream-processor --precompile
ENTRYPOINT ["./compiler-server", "--compiler-working-directory=/working-dir", "--sql-compiler-home=/database-stream-processor/sql-to-dbsp-compiler", "--dbsp-override-path=/database-stream-processor"]

# Standalone local-runner
FROM gcr.io/distroless/cc-debian12 AS local-runner
COPY --from=builder /app/target/release/local-runner local-runner
ENTRYPOINT ["./local-runner", "--runner-working-directory=/working-dir"]

# DB Migrations
FROM gcr.io/distroless/cc-debian12 AS migrations
COPY --from=builder /app/target/release/migrations migrations
ENTRYPOINT ["./migrations"]

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
FROM ubuntu:22.04 AS client
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
  && pip3 install snowflake-connector-python \
  # TODO: only required for running the fraud detection demo. Remove when we clean that up.
  && pip3 install gdown \
  && pip3 install "psycopg[binary]" \
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
