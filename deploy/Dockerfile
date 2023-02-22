FROM ubuntu:22.04

# Skip past interactive prompts during apt install
ENV DEBIAN_FRONTEND noninteractive

RUN apt update && apt install libssl-dev build-essential pkg-config \
     git gcc clang libclang-dev python3-pip hub numactl cmake \
     curl openjdk-19-jre-headless maven netcat jq \
     adduser libfontconfig1 unzip -y

RUN pip install gdown

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# Install rpk
RUN arch=`dpkg --print-architecture`; \
   curl -LO https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-$arch.zip \
   && unzip rpk-linux-$arch.zip -d /bin/ \
   && rpk version \
   && rm rpk-linux-$arch.zip


# Install Prometheus
RUN arch=`dpkg --print-architecture`; \
   curl -LO https://github.com/prometheus/prometheus/releases/download/v2.41.0/prometheus-2.41.0.linux-$arch.tar.gz \
   && tar xvfz prometheus-*.tar.gz \
   && cd prometheus-* \
   && mv prometheus /bin/ \
   && rm /prometheus-*.tar.gz

# Install grafana
RUN arch=`dpkg --print-architecture`; \
    curl -LO https://dl.grafana.com/enterprise/release/grafana-enterprise_9.3.6_$arch.deb \
    && dpkg -i grafana-enterprise_9.3.6_$arch.deb \
    && rm grafana-enterprise_9.3.6_$arch.deb

ADD ./dbsp_files.tar /database-stream-processor
ADD ./sql_compiler_files.tar /database-stream-processor/sql-to-dbsp-compiler

RUN cd /database-stream-processor/crates/pipeline_manager \
    && ~/.cargo/bin/cargo install --path . \
    && rm -rf /database-stream-processor/target

RUN cd /database-stream-processor/sql-to-dbsp-compiler/SQL-compiler && mvn -DskipTests package

# Provision Prometheus data source + DBSP dashboard in Grafana.
RUN cp /database-stream-processor/deploy/prometheus_datasource.yaml /etc/grafana/provisioning/datasources/ \
   && cp /database-stream-processor/deploy/grafana_dashboard_provision.yaml /etc/grafana/provisioning/dashboards/ \
   && cp /database-stream-processor/deploy/grafana_dashboard.json /etc/grafana/provisioning/dashboards/ \
   && mkdir /working-dir

ENV PATH="$PATH:/root/.cargo/bin"

CMD /database-stream-processor/scripts/start_prometheus.sh /working-dir && \
    /root/.cargo/bin/dbsp_pipeline_manager --bind-address=0.0.0.0 --working-directory=/working-dir --sql-compiler-home=/database-stream-processor/sql-to-dbsp-compiler --dbsp-override-path=/database-stream-processor
