FROM flink:latest
WORKDIR /opt/nexmark
COPY nexmark-flink .
COPY config.sh entrypoint.sh bin
COPY nexmark.yaml conf

WORKDIR /opt/flink
COPY run-nexmark.sh bin
COPY nexmark-flink/lib lib
COPY flink-sql-connector-kafka-1.17.0.jar lib
COPY flink-conf.yaml nexmark-flink/conf/sql-client-defaults.yaml conf

WORKDIR /opt/kafka
COPY kafka_2.13-3.4.0 .

ENTRYPOINT ["/opt/nexmark/bin/entrypoint.sh"]
CMD ["help"]
