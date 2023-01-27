FROM ubuntu:22.04

# Skip past interactive prompts during apt install
ENV DEBIAN_FRONTEND noninteractive

RUN apt update && apt install libssl-dev build-essential pkg-config \
     git gcc clang libclang-dev python3-pip hub numactl cmake \
     curl openjdk-19-jdk maven postgresql-client postgresql netcat jq \
     adduser libfontconfig1 unzip -y

RUN pip install gdown

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# Install rpk
RUN curl -LO https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-amd64.zip \
   && unzip rpk-linux-amd64.zip -d /bin/ \
   && rpk version

# Allow connections to postgres from localhost without a password.
RUN sed -i "s/scram-sha-256/trust/g" /etc/postgresql/14/main/pg_hba.conf \
   && service postgresql restart \
   && createuser -h localhost -U postgres dbsp

# Install Prometheus
RUN curl -LO https://github.com/prometheus/prometheus/releases/download/v2.41.0/prometheus-2.41.0.linux-amd64.tar.gz \
   && tar xvfz prometheus-*.tar.gz \
   && cd prometheus-* \
   && mv prometheus /bin/

# Install grafana
RUN curl -LO https://dl.grafana.com/enterprise/release/grafana-enterprise_9.3.6_amd64.deb \
   && dpkg -i grafana-enterprise_9.3.6_amd64.deb

ADD ./dbsp_files.tar /database-stream-processor
ADD ./sql_compiler_files.tar /sql-to-dbsp-compiler

RUN cd /database-stream-processor/pipeline_manager && ~/.cargo/bin/cargo build --release
RUN cd /sql-to-dbsp-compiler/SQL-compiler && mvn -DskipTests package

# Provision Prometheus data source + DBSP dashboard in Grafana.
RUN cp /database-stream-processor/deploy/prometheus_datasource.yaml /etc/grafana/provisioning/datasources/ \
   && cp /database-stream-processor/deploy/grafana_dashboard_provision.yaml /etc/grafana/provisioning/dashboards/ \
   && cp /database-stream-processor/deploy/grafana_dashboard.json /etc/grafana/provisioning/dashboards/

CMD service postgresql start && bash
