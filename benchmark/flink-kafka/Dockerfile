FROM flink:1.20
WORKDIR /opt/nexmark
COPY nexmark-flink .
COPY config.sh entrypoint.sh bin
COPY nexmark.yaml conf

WORKDIR /opt/flink
COPY run-nexmark.sh bin
COPY nexmark-flink/lib lib
COPY flink-sql-connector-kafka-3.2.0-1.19.jar lib
COPY flink-conf.yaml nexmark-flink/conf/sql-client-defaults.yaml conf

WORKDIR /opt/kafka
COPY kafka_2.13-3.8.0 .

ENTRYPOINT ["/opt/nexmark/bin/entrypoint.sh"]
CMD ["help"]
