FROM ubuntu:22.04

# Skip past interactive prompts during apt install
ENV DEBIAN_FRONTEND noninteractive

RUN apt update && apt install libssl-dev build-essential pkg-config \
     git gcc clang libclang-dev python3-pip python3-plumbum hub numactl cmake \
     curl openjdk-19-jre-headless maven netcat jq \
     adduser libfontconfig1 unzip -y

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

RUN ~/.cargo/bin/cargo install --no-default-features --force cargo-make

# Install rpk
RUN arch=`dpkg --print-architecture`; \
   curl -LO https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-$arch.zip \
   && unzip rpk-linux-$arch.zip -d /bin/ \
   && rpk version \
   && rm rpk-linux-$arch.zip

COPY . /database-stream-processor

# Update SQL compiler submodule to the version specified in the repo, unless
# the submodule is _not_ in detached head state, which indicates that the user
# is working on the submodule and wants to build a container with their modified
# SQL compiler version.
RUN cd /database-stream-processor && \
    if [[ ! -e sql-to-dbsp-compiler/.git || -z $(cd sql-to-dbsp-compiler && git branch --show-current) ]]; \
    then git submodule update --init; fi

RUN cd /database-stream-processor/crates/pipeline_manager \
    && ~/.cargo/bin/cargo make openapi_python \
    && ~/.cargo/bin/cargo install --path . \
    && rm -rf /database-stream-processor/target .git

RUN cd /database-stream-processor/sql-to-dbsp-compiler/SQL-compiler && mvn -DskipTests package

ENV PATH="$PATH:/root/.cargo/bin"

RUN /root/.cargo/bin/dbsp_pipeline_manager --bind-address=0.0.0.0 --working-directory=/working-dir --sql-compiler-home=/database-stream-processor/sql-to-dbsp-compiler --dbsp-override-path=/database-stream-processor --unix-daemon \
    && /database-stream-processor/demo/create_demo_projects.sh

CMD /root/.cargo/bin/dbsp_pipeline_manager --bind-address=0.0.0.0 --working-directory=/working-dir --sql-compiler-home=/database-stream-processor/sql-to-dbsp-compiler --dbsp-override-path=/database-stream-processor
