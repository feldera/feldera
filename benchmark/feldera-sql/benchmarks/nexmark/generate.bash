cargo run -p dbsp_nexmark --example generate --features with-kafka -- --max-events $MAX_EVENTS -O bootstrap.servers=$KAFKA_BROKER
