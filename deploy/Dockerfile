FROM ubuntu:latest

# Skip past interactive prompts during apt install
ENV DEBIAN_FRONTEND noninteractive

RUN apt update && apt install libssl-dev build-essential pkg-config \
     git gcc clang libclang-dev python3-pip hub numactl cmake \
     curl openjdk-19-jdk maven postgresql-client postgresql netcat jq unzip -y

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# Install rpk
RUN curl -LO https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-amd64.zip \
   && unzip rpk-linux-amd64.zip -d /bin/ \
   && rpk version

# Allow connections to postgres from localhost without a password.
RUN sed -i "s/scram-sha-256/trust/g" /etc/postgresql/14/main/pg_hba.conf \
   && service postgresql restart \
   && createuser -h localhost -U postgres dbsp

ADD ./dbsp_files.tar /database-stream-processor
ADD ./sql_compiler_files.tar /sql-to-dbsp-compiler

RUN cd /database-stream-processor && ~/.cargo/bin/cargo build --release
RUN cd /sql-to-dbsp-compiler/SQL-compiler && mvn -DskipTests package

CMD service postgresql start && bash
