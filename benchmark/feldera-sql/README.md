# Feldera Nexmark benchmark in SQL

Feldera includes two versions of the Nexmark benchmark: one
implemented in Rust and the other in SQL.  This is the SQL version of
the benchmark, which is less mature and less complete than the Rust
version.  To run the Rust version of the benchmark, use `cargo bench
--bench nexmark`.

There are three big steps:

1. Start a Kafka (or Redpanda) broker and bring up a Feldera instance.

2. Generate Nexmark input events into the Kafka broker.  This is how
   you control the number of input events in the benchmark.

3. Run the benchmark itself using the generated events.

The actual benchmark in step 3 can run multiple times using the same
data generated in step 2.

The following sections explain each step.

## Step 1: Start Kafka and Feldera

This step has the following sub-steps:

1. Start or obtain access to a Kafka (or Redpanda) broker.  One way to
   do that is to start a Redpanda container on the local system:

   ```
    docker run --name redpanda -p 9092:9092 --rm -itd docker.redpanda.com/vectorized/redpanda:v23.2.3 redpanda start --smp 2
   ```


2. Bring up a Feldera instance, e.g. by running (in a separate
   terminal):

   ```
   cargo run -p pipeline-manager --features pg-embed
   ```

3. Save the access methods to the Kafka broker and the API for the
   Feldera instance in shell variables:

   - `$KAFKA_BROKER`: The host and port to access the Kafka broker
     from the shell prompt.

   - `$KAFKA_FROM_FELDERA`: The host and port that Feldera can use to
     access the Kafka broker.  This might be different from
     `$KAFKA_BROKER` if Feldera is running in containers or VMs with
     an independent networking configuration.

   - `$FELDERA_API`: The URL for the Feldera API.

   If you used the commands above to start Redpanda and Feldera, these
   commands will set the shell variables correctly:

   ```
   KAFKA_BROKER=localhost:9092
   KAFKA_FROM_FELDERA=localhost:9092
   FELDERA_API=http://localhost:8080
   ```

## Step 2: Generate data into Kafka

This step has the following sub-steps:

1. Delete any existing events already generated into the Kafka broker,
   using the `rpk` utility from Redpanda.  If this is the first time
   you're generating events into this broker, then you can skip this
   step (but it doesn't hurt to run it):

   ```
   rpk topic -X brokers=$KAFKA_BROKER delete bid auction person
   ```

2. Generate events into the Kafka broker using a command like the
   following:

   ```
   cargo run  -p dbsp_nexmark --example generate --features with-kafka -- --max-events 10000000 -O bootstrap.servers=$KAFKA_BROKER
   ```

   The command above generates 10 million events.  This is a moderate
   number that you should feel free to increase or decrease.

3. Optionally, if you want to verify that the requested number of
   events was generated, you can use the following command using the
   `rpk` utility that comes with Redpanda.  It should print exactly
   the number pased as `--max-events` earlier:

   ```
   for topic in auction bid person; do rpk -X brokers=$KAFKA_BROKER topic consume -f '%v' -o :end $topic; done | wc -l
   ```

## Step 3: Run the benchmark

To run the benchmark itself, run:

```
python3 benchmark/feldera-sql/run.py --api-url $FELDERA_API --kafka-broker $KAFKA_FROM_FELDERA
```
