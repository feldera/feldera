VERSION --try 0.8
IMPORT github.com/earthly/lib/rust:1a4a008e271c7a5583e7bd405da8fd3624c05610 AS rust
FROM ubuntu:22.04

RUN apt-get update && apt-get install --yes sudo

WORKDIR /dbsp
ENV RUSTUP_HOME=$HOME/.rustup
ENV CARGO_HOME=$HOME/.cargo
# Adds python and rust binaries to thep path
ENV PATH=$HOME/.cargo/bin:$HOME/.local/bin:$PATH
ENV RUST_VERSION=1.78.0
ENV RUST_BUILD_MODE='' # set to --release for release builds
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

install-deps:
    RUN apt-get update
    RUN apt-get install --yes build-essential curl libssl-dev build-essential pkg-config \
                              cmake git gcc clang libclang-dev python3-pip python3-plumbum \
                              hub numactl openjdk-19-jre-headless maven netcat jq \
                              docker.io libenchant-2-2 graphviz locales protobuf-compiler
    # Set UTF-8 locale. Needed for the Rust compiler to handle Unicode column names.
    RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
        locale-gen
    ENV LC_ALL en_US.UTF-8
    ENV LANG en_US.UTF-8
    # NodeJS install
    RUN sudo apt-get install -y ca-certificates curl gnupg
    RUN sudo mkdir -p /etc/apt/keyrings
    RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | sudo gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
    ENV NODE_MAJOR=20
    RUN echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" | sudo tee /etc/apt/sources.list.d/nodesource.list
    RUN sudo apt-get update
    RUN sudo apt-get install nodejs -y
    RUN npm install --global yarn
    RUN npm install --global openapi-typescript-codegen

install-rust:
    FROM +install-deps
    RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- \
        -y \
        --default-toolchain $RUST_VERSION \
        --profile minimal \
        --component clippy \
        --component rustfmt \
        --component llvm-tools-preview
    RUN rustup toolchain install nightly \
        --component clippy \
        --component rustfmt \
        --component llvm-tools-preview
    RUN cargo install --locked --force --version 0.5.0 cargo-machete
    RUN cargo install --locked --force --version 0.36.11 cargo-make
    RUN cargo install --locked --force --version 0.5.22 cargo-llvm-cov
    RUN cargo install --locked --force --version 0.1.61 cargo-chef
    RUN rustup --version
    RUN cargo --version
    RUN rustc --version
    # Switch back to this once https://github.com/earthly/earthly/issues/3718 is resolved:
    # DO rust+INIT --keep_fingerprints=true
    # Do this until then:
    ARG EARTHLY_TARGET_PROJECT_NO_TAG
    ARG EARTHLY_GIT_BRANCH
    ARG OS_RELEASE=$(md5sum /etc/os-release | cut -d ' ' -f 1)
    DO rust+INIT --cache_prefix="${EARTHLY_TARGET_PROJECT_NO_TAG}#${EARTHLY_GIT_BRANCH}#${OS_RELEASE}#earthly-cargo-cache" --keep_fingerprints=true

rust-sources:
    FROM +install-rust
    COPY --keep-ts Cargo.toml Cargo.toml
    COPY --keep-ts Cargo.lock Cargo.lock
    COPY --keep-ts --dir crates crates
    COPY --keep-ts sql-to-dbsp-compiler/lib sql-to-dbsp-compiler/lib
    COPY --keep-ts README.md README.md

formatting-check:
    FROM +rust-sources
    DO rust+CARGO --args="+nightly fmt --all -- --check"

machete:
    FROM +rust-sources
    DO rust+CARGO --args="machete crates/"

clippy:
    FROM +rust-sources
    ENV WEBUI_BUILD_DIR=/dbsp/web-console/out
    COPY ( +build-webui/out ) ./web-console/out
    DO rust+CARGO --args="clippy -- -D warnings"
    ENV RUSTDOCFLAGS="-D warnings"
    DO rust+CARGO --args="doc --no-deps"

install-python-deps:
    FROM +install-deps
    RUN pip install wheel
    COPY demo/demo_notebooks/requirements.txt requirements.txt
    RUN pip wheel -r requirements.txt --wheel-dir=wheels
    SAVE ARTIFACT wheels /wheels

install-python:
    FROM +install-deps
    COPY +install-python-deps/wheels wheels
    COPY demo/demo_notebooks/requirements.txt requirements.txt
    RUN pip install --user -v --no-index --find-links=wheels -r requirements.txt
    SAVE ARTIFACT /root/.local/lib/python3.10
    SAVE ARTIFACT /root/.local/bin

build-webui-deps:
    FROM +install-deps
    COPY web-console/package.json ./web-console/package.json
    COPY web-console/yarn.lock ./web-console/yarn.lock

    RUN cd web-console && yarn install

build-webui:
    FROM +build-webui-deps
    COPY --dir web-console/public web-console/public
    COPY --dir web-console/src web-console/src
    COPY --dir demo/demos demo/demos
    COPY demo/demos.json demo/
    COPY web-console/.editorconfig web-console/
    COPY web-console/.eslintrc.json web-console/
    COPY web-console/.prettierrc.js web-console/
    COPY web-console/next-env.d.ts ./web-console/next-env.d.ts
    COPY web-console/next.config.js ./web-console/next.config.js
    COPY web-console/next.d.ts ./web-console/next.d.ts
    COPY web-console/tsconfig.json ./web-console/tsconfig.json
    COPY web-console/.env ./web-console/.env

    RUN cd web-console && yarn format-check
    RUN cd web-console && yarn build
    SAVE ARTIFACT ./web-console/out

build-dbsp:
    FROM +rust-sources
    DO rust+CARGO --args="build --package dbsp --benches"
    DO rust+CARGO --args="build --package pipeline_types"
    DO rust+CARGO --args="build --package dbsp_nexmark --benches"

build-sql:
    FROM +build-dbsp
    COPY --keep-ts sql-to-dbsp-compiler sql-to-dbsp-compiler
    COPY demo/hello-world/combiner.sql demo/hello-world/combiner.sql
    COPY demo/project_demo00-SecOps/project.sql demo/project_demo00-SecOps/project.sql
    COPY demo/project_demo01-TimeSeriesEnrich/project.sql demo/project_demo01-TimeSeriesEnrich/project.sql
    COPY demo/project_demo02-FraudDetection/project.sql demo/project_demo02-FraudDetection/project.sql
    COPY demo/project_demo03-GreenTrip/project.sql demo/project_demo03-GreenTrip/project.sql
    COPY demo/project_demo04-SimpleSelect/project.sql demo/project_demo04-SimpleSelect/project.sql
    CACHE /root/.m2
    RUN cd "sql-to-dbsp-compiler/SQL-compiler" && mvn package -DskipTests --no-transfer-progress
    SAVE ARTIFACT sql-to-dbsp-compiler/SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar sql2dbsp-jar-with-dependencies.jar
    SAVE ARTIFACT sql-to-dbsp-compiler

build-adapters:
    # Adapter integration tests use the SQL compiler.
    FROM +build-sql
    DO rust+CARGO --args="build --package dbsp_adapters"

build-manager:
    FROM +build-adapters
    # For some reason if this ENV before the FROM line it gets invalidated
    ENV WEBUI_BUILD_DIR=/dbsp/web-console/out
    COPY ( +build-webui/out ) ./web-console/out
    DO rust+CARGO --args="build --package pipeline-manager --features pg-embed" --output="debug/pipeline-manager"

    IF [ -f ./target/debug/pipeline-manager ]
        SAVE ARTIFACT --keep-ts ./target/debug/pipeline-manager pipeline-manager
    END
    IF [ -f ./target/release/pipeline-manager ]
        SAVE ARTIFACT --keep-ts ./target/release/pipeline-manager pipeline-manager
    END

test-sql:
    # SQL-generated code imports adapters crate.
    FROM +build-adapters
    RUN cd "sql-to-dbsp-compiler/SQL-compiler" && mvn package -q --no-transfer-progress

build-nexmark:
    FROM +build-dbsp
    DO rust+CARGO --args="build --package dbsp_nexmark"

test-dbsp:
    FROM +build-dbsp
    ENV RUST_BACKTRACE 1
    DO rust+CARGO --args="test --package dbsp"

test-nexmark:
    FROM +build-nexmark
    ENV RUST_BACKTRACE 1
    DO rust+CARGO --args="test  --package dbsp_nexmark"

test-adapters:
    FROM +build-adapters
    DO rust+SET_CACHE_MOUNTS_ENV
    ARG DELTA_TABLE_TEST_AWS_ACCESS_KEY_ID
    ARG DELTA_TABLE_TEST_AWS_SECRET_ACCESS_KEY
    WITH DOCKER --pull docker.redpanda.com/vectorized/redpanda:v23.2.3
        RUN --mount=$EARTHLY_RUST_CARGO_HOME_CACHE --mount=$EARTHLY_RUST_TARGET_CACHE docker run -p 9092:9092 --rm -itd docker.redpanda.com/vectorized/redpanda:v23.2.3 \
            redpanda start --smp 2  && \
            sleep 5 && \
            RUST_BACKTRACE=1 cargo test --package dbsp_adapters --package sqllib
    END

test-manager:
    FROM +build-manager
    ENV PGHOST=localhost
    ENV PGUSER=postgres
    ENV PGCLIENTENCODING=UTF8
    ENV PGPORT=5432
    ENV RUST_LOG=error
    DO rust+SET_CACHE_MOUNTS_ENV
    WITH DOCKER --pull postgres
        # We just put the PGDATA in /dev/shm because the docker fs seems very slow (test time goes to 2min vs. shm 40s)
        RUN --mount=$EARTHLY_RUST_CARGO_HOME_CACHE --mount=$EARTHLY_RUST_TARGET_CACHE docker run --shm-size=512MB -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust -e PGDATA=/dev/shm -d postgres && \
            # Sleep until postgres is up (otherwise we get connection reset if we connect too early)
            # (See: https://github.com/docker-library/docs/blob/master/postgres/README.md#caveats)
            sleep 3 && \
            RUST_BACKTRACE=1 cargo test --package pipeline-manager
    END
    # We keep the test binary around so we can run integration tests later. This incantation is used to find the
    # test binary path, adapted from: https://github.com/rust-lang/cargo/issues/3670
    RUN --mount=$EARTHLY_RUST_CARGO_HOME_CACHE --mount=$EARTHLY_RUST_TARGET_CACHE cp `cargo test --features integration-test --no-run --package pipeline-manager --message-format=json | jq -r 'select(.target.kind[0] == "lib") | .executable' | grep -v null` test_binary
    SAVE ARTIFACT test_binary

openapi-checker:
    FROM +build-manager

    # Copy over pipeline manager executable
    COPY +build-manager/pipeline-manager .

    # Copy over OpenAPI spec currently in the repository
    COPY openapi.json openapi.json-base

    # Below will fail if the OpenAPI spec which is freshly generated
    # differs from the one currently in the repository
    RUN mkdir checker
    RUN cd checker && ../pipeline-manager --dump-openapi &&  \
        diff -bur openapi.json ../openapi.json-base

test-python:
    FROM +build-manager
    COPY +build-manager/pipeline-manager .
    RUN mkdir -p /root/.local/lib/python3.10
    RUN mkdir -p /root/.local/bin

    COPY +install-python/python3.10 /root/.local/lib/python3.10
    COPY +install-python/bin /root/.local/bin

    COPY +build-manager/pipeline-manager .
    COPY +build-sql/sql-to-dbsp-compiler sql-to-dbsp-compiler

    COPY demo/demo_notebooks demo/demo_notebooks
    COPY demo/simple-join demo/simple-join

    # Reuse `Cargo.lock` to ensure consistent crate versions.
    RUN mkdir -p /working-dir/cargo_workspace
    COPY Cargo.lock /working-dir/cargo_workspace/Cargo.lock

    ENV PGHOST=localhost
    ENV PGUSER=postgres
    ENV PGCLIENTENCODING=UTF8
    ENV PGPORT=5432
    ENV RUST_LOG=error
    ENV WITH_POSTGRES=1
    ENV IN_CI=1
    WITH DOCKER --pull postgres
        RUN docker run --shm-size=512MB -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust -e PGDATA=/dev/shm -d postgres && \
            sleep 10 && \
            (./pipeline-manager --bind-address=0.0.0.0 --api-server-working-directory=/working-dir --compiler-working-directory=/working-dir --runner-working-directory=/working-dir --sql-compiler-home=/dbsp/sql-to-dbsp-compiler --dbsp-override-path=/dbsp --db-connection-string=postgresql://postgres:postgres@localhost:5432 --compilation-profile=unoptimized &) && \
            sleep 5 && \
            python3 demo/simple-join/run.py --api-url http://localhost:8080 && \
            cd demo/demo_notebooks && jupyter execute fraud_detection.ipynb --JupyterApp.log_level='DEBUG'
    END

test-rust:
    BUILD +test-dbsp
    BUILD +test-nexmark
    BUILD +test-adapters
    BUILD +test-manager

# TODO: the following two container tasks duplicate work that we otherwise do in the Dockerfile,
# but by mostly repeating ourselves, we can reuse earlier Earthly stages to speed up the CI.
build-pipeline-manager-container:
    FROM +install-deps
    RUN useradd -ms /bin/bash feldera
    USER feldera
    WORKDIR /home/feldera

    # First, copy over the artifacts built from previous stages
    RUN mkdir -p database-stream-processor/sql-to-dbsp-compiler/SQL-compiler/target
    COPY +build-manager/pipeline-manager .
    COPY +build-sql/sql2dbsp-jar-with-dependencies.jar database-stream-processor/sql-to-dbsp-compiler/SQL-compiler/target/

    # Reuse `Cargo.lock` to ensure consistent crate versions.
    RUN mkdir -p .feldera/cargo_workspace
    COPY --chown=feldera Cargo.lock .feldera/cargo_workspace/Cargo.lock

    # Then copy over the crates needed by the sql compiler
    COPY crates/dbsp database-stream-processor/crates/dbsp
    COPY crates/pipeline-types database-stream-processor/crates/pipeline-types
    COPY crates/adapters database-stream-processor/crates/adapters
    COPY README.md database-stream-processor/README.md

    # Then copy over the required SQL compiler files
    COPY sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp database-stream-processor/sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp
    COPY sql-to-dbsp-compiler/lib database-stream-processor/sql-to-dbsp-compiler/lib
    COPY sql-to-dbsp-compiler/temp database-stream-processor/sql-to-dbsp-compiler/temp
    ENV RUSTUP_HOME=$HOME/.rustup
    ENV CARGO_HOME=$HOME/.cargo

    # Install cargo and rust for this non-root user
    RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
    ENV PATH="$PATH:/home/feldera/.cargo/bin"
    RUN ./pipeline-manager --bind-address=0.0.0.0 --sql-compiler-home=/home/feldera/database-stream-processor/sql-to-dbsp-compiler --compilation-profile=unoptimized --dbsp-override-path=/home/feldera/database-stream-processor --precompile
    ENTRYPOINT ["./pipeline-manager", "--bind-address=0.0.0.0", "--sql-compiler-home=/home/feldera/database-stream-processor/sql-to-dbsp-compiler", "--dbsp-override-path=/home/feldera/database-stream-processor", "--compilation-profile=unoptimized"]

# Same as the above, but with a permissive CORS setting, else playwright doesn't work
pipeline-manager-container-cors-all:
    FROM +build-pipeline-manager-container
    ENTRYPOINT ["./pipeline-manager", "--bind-address=0.0.0.0", "--sql-compiler-home=/home/feldera/database-stream-processor/sql-to-dbsp-compiler", "--dbsp-override-path=/home/feldera/database-stream-processor", "--dev-mode", "--compilation-profile=unoptimized"]

# TODO: mirrors the Dockerfile. See note above.
build-demo-container:
    FROM +install-rust
    WORKDIR /
    RUN apt install unzip -y
    RUN arch=`dpkg --print-architecture`; \
            curl -LO https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-$arch.zip \
            && unzip rpk-linux-$arch.zip -d /bin/ \
            && rpk version \
            && rm rpk-linux-$arch.zip
    # Install snowsql
    RUN curl -O https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/1.2/linux_x86_64/snowsql-1.2.28-linux_x86_64.bash \
        && SNOWSQL_DEST=/bin SNOWSQL_LOGIN_SHELL=~/.profile bash snowsql-1.2.28-linux_x86_64.bash \
        && snowsql -v
    COPY +install-python/python3.10 /root/.local/lib/python3.10
    COPY +install-python/bin /root/.local/bin
    # Needed by the JDBC demo.
    RUN pip3 install "psycopg[binary]"
    # Needed by the simple-count demo.
    RUN pip3 install kafka-python
    COPY demo demo
    CMD bash

build-kafka-connect-container:
    FROM DOCKERFILE -f deploy/Dockerfile --target kafka-connect .

test-docker-compose:
    FROM earthly/dind:alpine
    COPY deploy/docker-compose.yml .
    ENV FELDERA_VERSION=latest
    WITH DOCKER --pull docker.redpanda.com/vectorized/redpanda:v23.2.3 \
                --load ghcr.io/feldera/pipeline-manager:latest=+build-pipeline-manager-container \
                --load ghcr.io/feldera/demo-container:latest=+build-demo-container
        RUN COMPOSE_HTTP_TIMEOUT=120 SECOPS_DEMO_ARGS="--prepare-args 200000" RUST_LOG=debug,tokio_postgres=info docker-compose -f docker-compose.yml --profile demo up --force-recreate --exit-code-from demo
    END

# Test whether the stable container image runs with our Docker compose file
# and whether we can migrate from the last stable version to the latest version
test-docker-compose-stable:
    FROM earthly/dind:alpine
    COPY deploy/docker-compose.yml .
    ENV FELDERA_VERSION=0.17.0
    RUN apk --no-cache add curl
    WITH DOCKER --pull postgres \
                --pull docker.redpanda.com/vectorized/redpanda:v23.2.3 \
                --pull ghcr.io/feldera/pipeline-manager:0.17.0 \
                --load ghcr.io/feldera/pipeline-manager:latest=+build-pipeline-manager-container \
                --pull ghcr.io/feldera/demo-container:0.17.0
        RUN COMPOSE_HTTP_TIMEOUT=120 SECOPS_DEMO_ARGS="--prepare-args 200000" RUST_LOG=debug,tokio_postgres=info docker-compose -f docker-compose.yml --profile demo up --force-recreate --exit-code-from demo && \
            # This should run the latest version of the code and in the process, trigger a migration.
            COMPOSE_HTTP_TIMEOUT=120 SECOPS_DEMO_ARGS="--prepare-args 200000" FELDERA_VERSION=latest RUST_LOG=debug,tokio_postgres=info docker-compose -f docker-compose.yml up -d db pipeline-manager redpanda && \
            sleep 10 && \
            # Exercise a few simple workflows in the API
            curl http://localhost:8080/v0/programs &&  \
            curl http://localhost:8080/v0/pipelines &&  \
            curl http://localhost:8080/v0/connectors
    END

test-debezium-mysql:
    FROM earthly/dind:alpine
    COPY deploy/docker-compose.yml .
    COPY deploy/docker-compose-debezium.yml .
    ENV FELDERA_VERSION=latest
    WITH DOCKER --pull docker.redpanda.com/vectorized/redpanda:v23.2.3 \
                --pull debezium/example-mysql:2.5 \
                --load ghcr.io/feldera/pipeline-manager:latest=+build-pipeline-manager-container \
                --load ghcr.io/feldera/demo-container:latest=+build-demo-container \
                --load ghcr.io/feldera/kafka-connect:latest=+build-kafka-connect-container
        RUN COMPOSE_HTTP_TIMEOUT=120 RUST_LOG=debug,tokio_postgres=info docker-compose -f docker-compose.yml -f docker-compose-debezium.yml --profile debezium up --force-recreate --exit-code-from debezium-demo
    END

test-debezium-jdbc-sink:
    FROM earthly/dind:alpine
    COPY deploy/docker-compose.yml .
    COPY deploy/docker-compose-jdbc.yml .
    ENV FELDERA_VERSION=latest
    WITH DOCKER --pull docker.redpanda.com/vectorized/redpanda:v23.2.3 \
                --pull debezium/example-postgres:2.3 \
                --load ghcr.io/feldera/pipeline-manager:latest=+build-pipeline-manager-container \
                --load ghcr.io/feldera/demo-container:latest=+build-demo-container \
                --load ghcr.io/feldera/kafka-connect:latest=+build-kafka-connect-container
        RUN COMPOSE_HTTP_TIMEOUT=120 RUST_LOG=debug,tokio_postgres=info docker-compose -f docker-compose.yml -f docker-compose-jdbc.yml --profile debezium up --force-recreate --exit-code-from debezium-jdbc-demo
    END

test-snowflake:
    FROM earthly/dind:alpine
    COPY deploy/docker-compose.yml .
    COPY deploy/.env .
    RUN cat .env
    ENV FELDERA_VERSION=latest
    WITH DOCKER --pull docker.redpanda.com/vectorized/redpanda:v23.2.3 \
                --load ghcr.io/feldera/pipeline-manager:latest=+build-pipeline-manager-container \
                --load ghcr.io/feldera/demo-container:latest=+build-demo-container \
                --load ghcr.io/feldera/kafka-connect:latest=+build-kafka-connect-container
        RUN COMPOSE_HTTP_TIMEOUT=120 RUST_LOG=debug,tokio_postgres=info docker-compose --env-file .env -f docker-compose.yml --profile snowflake up --force-recreate --exit-code-from snowflake-demo
    END

test-s3:
    FROM earthly/dind:alpine
    COPY deploy/docker-compose.yml .
    COPY deploy/.env .
    RUN cat .env
    ENV FELDERA_VERSION=latest
    WITH DOCKER --pull docker.redpanda.com/vectorized/redpanda:v23.2.3 \
                --load ghcr.io/feldera/pipeline-manager:latest=+build-pipeline-manager-container \
                --load ghcr.io/feldera/demo-container:latest=+build-demo-container \
                --load ghcr.io/feldera/kafka-connect:latest=+build-kafka-connect-container
        RUN COMPOSE_HTTP_TIMEOUT=120 RUST_LOG=debug,tokio_postgres=info docker-compose --env-file .env -f docker-compose.yml --profile s3 up --force-recreate --exit-code-from s3-demo
    END

test-service-related:
    FROM earthly/dind:alpine
    COPY deploy/docker-compose.yml .
    COPY deploy/.env .
    RUN cat .env
    ENV FELDERA_VERSION=latest
    WITH DOCKER --pull docker.redpanda.com/vectorized/redpanda:v23.2.3 \
                --load ghcr.io/feldera/pipeline-manager:latest=+build-pipeline-manager-container \
                --load ghcr.io/feldera/demo-container:latest=+build-demo-container
        RUN COMPOSE_HTTP_TIMEOUT=120 RUST_LOG=debug,tokio_postgres=info docker-compose --env-file .env -f docker-compose.yml --profile demo-service-related up --force-recreate --exit-code-from demo-service-related
    END

# Fetches the test binary from test-manager, and produces a container image out of it
integration-test-container:
    FROM +install-deps
    COPY +test-manager/test_binary .
    # Check that the binary does indeed run integration tests
    RUN ./test_binary integration_test --list | grep integration_test
    ENV TEST_DBSP_URL=http://pipeline-manager:8080
    ENV RUST_BACKTRACE=1
    ENTRYPOINT ["./test_binary", "integration_test::"]
    SAVE IMAGE itest:latest

# Runs the integration test container against the docker compose setup
integration-tests:
    FROM earthly/dind:alpine
    COPY deploy/docker-compose.yml .
    COPY deploy/.env .
    ENV FELDERA_VERSION=latest
    WITH DOCKER --load ghcr.io/feldera/pipeline-manager:latest=+build-pipeline-manager-container \
                --compose docker-compose.yml \
                --service pipeline-manager \
                --load itest:latest=+integration-test-container
        RUN sleep 15 && docker run --env-file .env --network default_default itest:latest
    END

ui-playwright-container:
    FROM +install-deps
    COPY web-console .
    COPY deploy/docker-compose.yml .
    COPY deploy/.env .

    # Pull playwright-snapshots for visual regression testing
    # It was decided it's better to clone the snapshots repo during the build rather than have it as a submodule
    ARG PLAYWRIGHT_SNAPSHOTS_COMMIT
    RUN echo PLAYWRIGHT_SNAPSHOTS_COMMIT=$PLAYWRIGHT_SNAPSHOTS_COMMIT
    GIT CLONE --branch=$PLAYWRIGHT_SNAPSHOTS_COMMIT https://github.com/feldera/playwright-snapshots.git playwright-snapshots

    WORKDIR web-console
    RUN yarn install
    RUN yarn playwright install
    RUN yarn playwright install-deps
    ENV CI=true
    ENV PLAYWRIGHT_API_ORIGIN=http://localhost:8080/
    ENV PLAYWRIGHT_APP_ORIGIN=http://localhost:8080/
    ENV DISPLAY=

    # Install docker compose - earthly can do this automatically, but it installs an older version
    ENV DOCKER_CONFIG=${DOCKER_CONFIG:-$HOME/.docker}
    RUN mkdir -p $DOCKER_CONFIG/cli-plugins
    RUN curl -SL https://github.com/docker/compose/releases/download/v2.24.0-birthday.10/docker-compose-linux-x86_64 -o $DOCKER_CONFIG/cli-plugins/docker-compose
    RUN chmod +x $DOCKER_CONFIG/cli-plugins/docker-compose

    # Install zip to prepare test artifacts for export
    RUN apt-get install -y zip

ui-playwright-tests:
    FROM +ui-playwright-container
    ENV FELDERA_VERSION=latest

    TRY
        WITH DOCKER --load ghcr.io/feldera/pipeline-manager:latest=+pipeline-manager-container-cors-all \
                    --compose ../docker-compose.yml \
                    --service pipeline-manager
            # We zip artifacts regardless of test success or error, and then we complete the command preserving test's exit_code
            RUN if yarn playwright test; then exit_code=0; else exit_code=$?; fi \
                && cd /dbsp \
                && zip -r playwright-report.zip playwright-report \
                && zip -r test-results.zip test-results \
                && exit $exit_code
        END
    FINALLY
        SAVE ARTIFACT --if-exists /dbsp/playwright-report.zip AS LOCAL ./playwright-artifacts/
        SAVE ARTIFACT --if-exists /dbsp/test-results.zip      AS LOCAL ./playwright-artifacts/
    END

benchmark:
    FROM +build-nexmark
    COPY scripts/bench.bash scripts/bench.bash

    RUN bash scripts/bench.bash
    SAVE ARTIFACT crates/nexmark/nexmark_results.csv AS LOCAL .
    SAVE ARTIFACT crates/nexmark/dram_nexmark_results.csv AS LOCAL .
    SAVE ARTIFACT crates/dbsp/galen_results.csv AS LOCAL .
    #SAVE ARTIFACT crates/dbsp/ldbc_results.csv AS LOCAL .

all-tests:
    BUILD +formatting-check
    BUILD +machete
    BUILD +clippy
    BUILD +test-rust
    BUILD +test-python
    BUILD +openapi-checker
    BUILD +test-sql
    BUILD +integration-tests
    # BUILD +ui-playwright-tests
    BUILD +test-docker-compose
    # BUILD +test-docker-compose-stable
    BUILD +test-debezium-mysql
    BUILD +test-debezium-jdbc-sink
    # BUILD +test-snowflake
    BUILD +test-s3
    BUILD +test-service-related
