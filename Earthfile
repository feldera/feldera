VERSION --global-cache 0.7
IMPORT github.com/earthly/lib/rust:52e8c8a1fe7e8364b7a28eeaca3e3525cee03cf6 AS rust
FROM ubuntu:22.04

RUN apt-get update && apt-get install --yes sudo

WORKDIR /dbsp
ENV RUSTUP_HOME=$HOME/.rustup
ENV CARGO_HOME=$HOME/.cargo
# Adds python and rust binaries to thep path
ENV PATH=$HOME/.cargo/bin:$HOME/.local/bin:$PATH
ENV RUST_VERSION=1.73.0
ENV RUST_BUILD_MODE='' # set to --release for release builds

install-deps:
    RUN apt-get update
    RUN apt-get install --yes build-essential curl libssl-dev build-essential pkg-config \
                              cmake git gcc clang libclang-dev python3-pip python3-plumbum \
                              hub numactl openjdk-19-jre-headless maven netcat jq \
                              docker.io libenchant-2-2 graphviz locales
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
    DO rust+INIT --keep_fingerprints=true

rust-sources:
    FROM +install-rust
    COPY --keep-ts Cargo.toml Cargo.toml
    COPY --keep-ts Cargo.lock Cargo.lock
    COPY --keep-ts --dir crates crates
    COPY --keep-ts sql-to-dbsp-compiler/lib sql-to-dbsp-compiler/lib
    COPY --keep-ts README.md README.md

formatting-check:
    FROM +rust-sources
    COPY --keep-ts rustfmt.toml rustfmt.toml
    DO rust+CARGO --args="+nightly  fmt --all -- --check"

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
    COPY python python
    RUN cd python && pip3 install --user -v ./feldera-api-client
    RUN cd python && pip3 install --user -v .
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
    COPY web-console/.editorconfig web-console/
    COPY web-console/.eslintrc.json web-console/
    COPY web-console/.prettierrc.js web-console/
    COPY web-console/next-env.d.ts ./web-console/next-env.d.ts
    COPY web-console/next.config.js ./web-console/next.config.js
    COPY web-console/next.d.ts ./web-console/next.d.ts
    COPY web-console/tsconfig.json ./web-console/tsconfig.json
    COPY web-console/.env ./web-console/.env

    RUN cd web-console && yarn format:check
    RUN cd web-console && yarn build
    SAVE ARTIFACT ./web-console/out

build-dbsp:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE
    FROM +rust-sources --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    DO rust+CARGO --args="build --package dbsp"
    DO rust+CARGO --args="clippy --package dbsp -- -D warnings"
    DO rust+CARGO --args="test --package dbsp --no-run" 
    DO rust+CARGO --args="build --package pipeline_types"
    DO rust+CARGO --args="clippy --package pipeline_types -- -D warnings"
    DO rust+CARGO --args="test --package pipeline_types --no-run" 

build-sql:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-dbsp --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    COPY --keep-ts sql-to-dbsp-compiler sql-to-dbsp-compiler

    COPY demo/hello-world/combiner.sql demo/hello-world/combiner.sql
    COPY demo/project_demo00-SecOps/project.sql demo/project_demo00-SecOps/project.sql
    COPY demo/project_demo01-TimeSeriesEnrich/project.sql demo/project_demo01-TimeSeriesEnrich/project.sql
    COPY demo/project_demo02-FraudDetection/project.sql demo/project_demo02-FraudDetection/project.sql
    COPY demo/project_demo03-GreenTrip/project.sql demo/project_demo03-GreenTrip/project.sql
    COPY demo/project_demo04-SimpleSelect/project.sql demo/project_demo04-SimpleSelect/project.sql

    CACHE /root/.m2

    COPY sql-to-dbsp-compiler sql-to-dbsp-compiler
    RUN cd "sql-to-dbsp-compiler/SQL-compiler" && mvn package -DskipTests --no-transfer-progress
    SAVE ARTIFACT sql-to-dbsp-compiler/SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar sql2dbsp-jar-with-dependencies.jar
    SAVE ARTIFACT sql-to-dbsp-compiler

build-adapters:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    # Adapter integration tests use the SQL compiler.
    FROM +build-sql --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    DO rust+CARGO --args="build --package dbsp_adapters"
    DO rust+CARGO --args="clippy --package dbsp_adapters -- -D warnings"
    DO rust+CARGO --args="test --package dbsp_adapters --no-run" 

build-manager:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-adapters --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    # For some reason if this ENV before the FROM line it gets invalidated
    ENV WEBUI_BUILD_DIR=/dbsp/web-console/out
    COPY ( +build-webui/out ) ./web-console/out
    DO rust+CARGO --args="build --package pipeline-manager" --output="debug/pipeline-manager"
    DO rust+CARGO --args="clippy --package pipeline-manager -- -D warnings"
    DO rust+CARGO --args="test --package pipeline-manager --no-run"

    IF [ -f ./target/debug/pipeline-manager ]
        SAVE ARTIFACT --keep-ts ./target/debug/pipeline-manager pipeline-manager
    END
    IF [ -f ./target/release/pipeline-manager ]
        SAVE ARTIFACT --keep-ts ./target/release/pipeline-manager pipeline-manager
    END

test-sql:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    # SQL-generated code imports adapters crate.
    FROM +build-adapters --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    RUN cd "sql-to-dbsp-compiler/SQL-compiler" && mvn package --no-transfer-progress

build-nexmark:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE
    FROM +build-dbsp --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    DO rust+CARGO --args="build --package dbsp_nexmark" --output="debug/[^/\.]+"
    DO rust+CARGO --args="clippy --package dbsp_nexmark -- -D warnings"
    DO rust+CARGO --args="test --package dbsp_nexmark --no-run"

CARGO_TEST:
    COMMAND
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE
    ARG package
    ARG features
    ARG test_args
    DO rust+CARGO --args="+$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package $package \
        $(if [ -z $features ]; then printf -- --features $features; fi) \
        -- $test_args"

test-dbsp:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-dbsp --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    # Limit test execution to tests in trace::persistent::tests, because
    # executing everything takes too long and (in theory) the proptests we have
    # should ensure equivalence with the DRAM trace implementation:
    DO +CARGO_TEST \
        --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE \
        --package=dbsp --features=persistence --test_args=trace::persistent::tests
    DO rust+CARGO --args="+$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dbsp"

test-nexmark:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-nexmark --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    DO rust+CARGO --args="+$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE  --package dbsp_nexmark"

test-adapters:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE
    FROM +build-adapters --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    DO rust+SET_CACHE_MOUNTS_ENV
    WITH DOCKER --pull docker.redpanda.com/vectorized/redpanda:v23.2.3
        RUN --mount=$EARTHLY_RUST_CARGO_HOME_CACHE --mount=$EARTHLY_RUST_TARGET_CACHE docker run -p 9092:9092 --rm -itd docker.redpanda.com/vectorized/redpanda:v23.2.3 \
            redpanda start --smp 2  && \
            sleep 5 && \
            # XXX: DO rust+CARGO --args="+$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dbsp_adapters --package sqllib"
            cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package dbsp_adapters --package sqllib
    END

test-manager:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-manager --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
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
            # XXX: DO rust+CARGO --args="+$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package pipeline-manager"
            cargo +$RUST_TOOLCHAIN test $RUST_BUILD_PROFILE --package pipeline-manager
    END
    # We keep the test binary around so we can run integration tests later. This incantation is used to find the
    # test binary path, adapted from: https://github.com/rust-lang/cargo/issues/3670
    RUN --mount=$EARTHLY_RUST_CARGO_HOME_CACHE --mount=$EARTHLY_RUST_TARGET_CACHE cp `cargo test --features integration-test --no-run --package pipeline-manager --message-format=json | jq -r 'select(.target.kind[0] == "lib") | .executable' | grep -v null` test_binary
    SAVE ARTIFACT test_binary

python-bindings-checker:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-manager --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    COPY +build-manager/pipeline-manager .
    RUN mkdir -p /root/.local/lib/python3.10
    RUN mkdir -p /root/.local/bin

    COPY +install-python/python3.10 /root/.local/lib/python3.10
    COPY +install-python/bin /root/.local/bin

    RUN pip3 install openapi-python-client==0.15.0 && openapi-python-client --version
    COPY +build-manager/pipeline-manager .
    COPY python/feldera-api-client feldera-api-client-base

    # This line will fail if the python bindings need to be regenerated
    RUN mkdir checker
    RUN cd checker && ../pipeline-manager --dump-openapi &&  \
        openapi-python-client generate --path openapi.json --fail-on-warning && \
        diff -bur feldera-api-client ../feldera-api-client-base


test-python:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-manager --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    COPY +build-manager/pipeline-manager .
    RUN mkdir -p /root/.local/lib/python3.10
    RUN mkdir -p /root/.local/bin

    COPY +install-python/python3.10 /root/.local/lib/python3.10
    COPY +install-python/bin /root/.local/bin

    COPY +build-manager/pipeline-manager .
    COPY +build-sql/sql-to-dbsp-compiler sql-to-dbsp-compiler

    COPY demo/demo_notebooks demo/demo_notebooks
    COPY python/test.py python/test.py

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
            (./pipeline-manager --bind-address=0.0.0.0 --api-server-working-directory=/working-dir --compiler-working-directory=/working-dir --runner-working-directory=/working-dir --sql-compiler-home=/dbsp/sql-to-dbsp-compiler --dbsp-override-path=/dbsp --db-connection-string=postgresql://postgres:postgres@localhost:5432 &) && \
            sleep 5 && \
            python3 python/test.py && \
            cd demo/demo_notebooks && jupyter execute fraud_detection.ipynb --JupyterApp.log_level='DEBUG'
    END

test-rust:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    BUILD +test-dbsp --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    BUILD +test-nexmark --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    BUILD +test-adapters --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    BUILD +test-manager --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE

# TODO: the following two container tasks duplicate work that we otherwise do in the Dockerfile,
# but by mostly repeating ourselves, we can reuse earlier Earthly stages to speed up the CI.
build-pipeline-manager-container:
    FROM +install-rust
    WORKDIR /

    # First, copy over the artifacts built from previous stages
    RUN mkdir -p database-stream-processor/sql-to-dbsp-compiler/SQL-compiler/target
    COPY +build-manager/pipeline-manager .
    COPY +build-sql/sql2dbsp-jar-with-dependencies.jar database-stream-processor/sql-to-dbsp-compiler/SQL-compiler/target/

    # Then copy over the crates needed by the sql compiler
    COPY crates/dbsp database-stream-processor/crates/dbsp
    COPY crates/pipeline-types database-stream-processor/crates/pipeline-types
    COPY crates/adapters database-stream-processor/crates/adapters
    COPY README.md database-stream-processor/README.md

    # Then copy over the required SQL compiler files
    COPY sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp /database-stream-processor/sql-to-dbsp-compiler/SQL-compiler/sql-to-dbsp
    COPY sql-to-dbsp-compiler/lib /database-stream-processor/sql-to-dbsp-compiler/lib
    COPY sql-to-dbsp-compiler/temp /database-stream-processor/sql-to-dbsp-compiler/temp
    RUN ./pipeline-manager --bind-address=0.0.0.0 --api-server-working-directory=/working-dir --compiler-working-directory=/working-dir --runner-working-directory=/working-dir --sql-compiler-home=/database-stream-processor/sql-to-dbsp-compiler --dbsp-override-path=/database-stream-processor --precompile
    ENTRYPOINT ["./pipeline-manager", "--bind-address=0.0.0.0", "--api-server-working-directory=/working-dir", "--compiler-working-directory=/working-dir", "--runner-working-directory=/working-dir", "--sql-compiler-home=/database-stream-processor/sql-to-dbsp-compiler", "--dbsp-override-path=/database-stream-processor"]

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
    COPY demo demo
    CMD bash

build-kafka-connect-container:
    FROM DOCKERFILE -f deploy/Dockerfile --target kafka-connect .

test-docker-compose:
    FROM earthly/dind:alpine
    COPY deploy/docker-compose.yml .
    ENV FELDERA_VERSION=latest
    WITH DOCKER --pull postgres \
                --pull docker.redpanda.com/vectorized/redpanda:v23.2.3 \
                --load ghcr.io/feldera/pipeline-manager:latest=+build-pipeline-manager-container \
                --load ghcr.io/feldera/demo-container:latest=+build-demo-container
        RUN COMPOSE_HTTP_TIMEOUT=120 SECOPS_DEMO_ARGS="--prepare-args 200000" RUST_LOG=debug,tokio_postgres=info docker-compose -f docker-compose.yml --profile demo up --force-recreate --exit-code-from demo
    END

# Test whether the stable container image runs with our Docker compose file
# and whether we can migrate from the last stable version to the latest version
test-docker-compose-stable:
    FROM earthly/dind:alpine
    COPY deploy/docker-compose.yml .
    ENV FELDERA_VERSION=0.6.0
    RUN apk --no-cache add curl
    WITH DOCKER --pull postgres \
                --pull docker.redpanda.com/vectorized/redpanda:v23.2.3 \
                --pull ghcr.io/feldera/pipeline-manager:0.6.0 \
                --load ghcr.io/feldera/pipeline-manager:latest=+build-pipeline-manager-container \
                --pull ghcr.io/feldera/demo-container:0.6.0
        RUN COMPOSE_HTTP_TIMEOUT=120 SECOPS_DEMO_ARGS="--prepare-args 200000" RUST_LOG=debug,tokio_postgres=info docker-compose -f docker-compose.yml --profile demo up --force-recreate --exit-code-from demo && \
            # This should run the latest version of the code and in the process, trigger a migration.
            COMPOSE_HTTP_TIMEOUT=120 SECOPS_DEMO_ARGS="--prepare-args 200000" FELDERA_VERSION=latest RUST_LOG=debug,tokio_postgres=info docker-compose -f docker-compose.yml up -d db pipeline-manager redpanda && \
            sleep 10 && \
            # Exercise a few simple workflows in the API
            curl http://localhost:8080/v0/programs &&  \
            curl http://localhost:8080/v0/pipelines &&  \
            curl http://localhost:8080/v0/connectors
    END

test-debezium:
    FROM earthly/dind:alpine
    COPY deploy/docker-compose.yml .
    COPY deploy/docker-compose-debezium.yml .
    ENV FELDERA_VERSION=latest
    WITH DOCKER --pull postgres \
                --pull docker.redpanda.com/vectorized/redpanda:v23.2.3 \
                --pull debezium/example-mysql:2.3 \
                --load ghcr.io/feldera/pipeline-manager:latest=+build-pipeline-manager-container \
                --load ghcr.io/feldera/demo-container:latest=+build-demo-container \
                --load ghcr.io/feldera/kafka-connect:latest=+build-kafka-connect-container
        RUN COMPOSE_HTTP_TIMEOUT=120 RUST_LOG=debug,tokio_postgres=info docker-compose -f docker-compose.yml -f docker-compose-debezium.yml --profile debezium up --force-recreate --exit-code-from debezium-demo
    END

test-snowflake:
    FROM earthly/dind:alpine
    COPY deploy/docker-compose.yml .
    COPY deploy/.env .
    RUN cat .env
    ENV FELDERA_VERSION=latest
    WITH DOCKER --pull postgres \
                --pull docker.redpanda.com/vectorized/redpanda:v23.2.3 \
                --load ghcr.io/feldera/pipeline-manager:latest=+build-pipeline-manager-container \
                --load ghcr.io/feldera/demo-container:latest=+build-demo-container \
                --load ghcr.io/feldera/kafka-connect:latest=+build-kafka-connect-container
        RUN COMPOSE_HTTP_TIMEOUT=120 RUST_LOG=debug,tokio_postgres=info docker-compose --env-file .env -f docker-compose.yml --profile snowflake up --force-recreate --exit-code-from snowflake-demo
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
    WITH DOCKER --pull postgres \
                --load ghcr.io/feldera/pipeline-manager:latest=+build-pipeline-manager-container \
                --compose docker-compose.yml \
                --service db \
                --service pipeline-manager \
                --load itest:latest=+integration-test-container
        RUN sleep 5 && docker run --env-file .env --network default_default itest:latest
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
    GIT CLONE https://github.com/feldera/playwright-snapshots.git playwright-snapshots
    # [ -n "..." ] syntax returns true if the string is not empty, in which case the commands after `&&` execute
    RUN [ -n "$PLAYWRIGHT_SNAPSHOTS_COMMIT" ] && cd playwright-snapshots && git checkout $PLAYWRIGHT_SNAPSHOTS_COMMIT

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
        WITH DOCKER --pull postgres \
                    --load ghcr.io/feldera/pipeline-manager:latest=+build-pipeline-manager-container \
                    --compose ../docker-compose.yml \
                    --service db \
                    --service pipeline-manager
            # We zip artifacts regardless of test success or error, and then we complete the command preserving test's exit_code
            RUN if yarn playwright test; then exit_code=0; else exit_code=$?; fi \
                && cd /dbsp \
                && zip -r playwright-report.zip playwright-report \
                && zip -r test-results.zip test-results \
                && exit $exit_code
        END
    FINALLY
        SAVE ARTIFACT --if-exists playwright-report.zip AS LOCAL ./playwright-artifacts/
        SAVE ARTIFACT --if-exists test-results.zip      AS LOCAL ./playwright-artifacts/
    END

benchmark:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    ARG RUST_BUILD_PROFILE=$RUST_BUILD_MODE

    FROM +build-nexmark --RUST_TOOLCHAIN=$RUST_TOOLCHAIN --RUST_BUILD_PROFILE=$RUST_BUILD_PROFILE
    COPY scripts/bench.bash scripts/bench.bash

    RUN bash scripts/bench.bash
    SAVE ARTIFACT crates/nexmark/nexmark_results.csv AS LOCAL .
    SAVE ARTIFACT crates/nexmark/dram_nexmark_results.csv AS LOCAL .
    SAVE ARTIFACT crates/nexmark/persistence_nexmark_results.csv AS LOCAL .
    SAVE ARTIFACT crates/dbsp/galen_results.csv AS LOCAL .
    SAVE ARTIFACT crates/dbsp/ldbc_results.csv AS LOCAL .

all-tests:
    BUILD +formatting-check
    BUILD +test-rust
    BUILD +test-python
    BUILD +python-bindings-checker
    BUILD +test-sql
    BUILD +test-docker-compose
    BUILD +test-docker-compose-stable
    BUILD +test-debezium
    BUILD +test-snowflake
    BUILD +integration-tests
    BUILD +ui-playwright-tests

