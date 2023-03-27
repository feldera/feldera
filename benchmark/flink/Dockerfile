FROM flink:latest
WORKDIR /opt/nexmark
COPY nexmark-flink .
COPY config.sh entrypoint.sh bin
COPY nexmark.yaml conf

WORKDIR /opt/flink
COPY run-nexmark.sh bin
COPY nexmark-flink/lib lib
COPY flink-conf.yaml nexmark-flink/conf/sql-client-defaults.yaml conf

ENTRYPOINT ["/opt/nexmark/bin/entrypoint.sh"]
CMD ["help"]
