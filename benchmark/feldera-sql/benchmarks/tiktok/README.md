# Running the Tiktok Benchmark


1. Ensure that you have Feldera is running.
2. Ensure that you have Kafka running.
3. Set the environment variable `MAX_EVENTS` to the number of events to generate.
   `MAX_EVENTS=100000000`
4. Set the environment variable `BOOTSTRAP_SERVERS` to point to the Kafka port
   `BOOTSTRAP_SERVERS=localhost:9092`
5. Run `generate.bash`. (`source generate.bash`)
6. Run the `feldera/benchmark/feldera-sql/run.py` script as follows:

```sh
cd feldera/benchmark/feldera-sql
python run.py --api-url http://localhost:8080 -O bootstrap.servers=localhost:9092 --csv tiktok.csv --csv-metrics tiktok-metrics.csv --metrics-interval 1 --poller-threads 10 --folder benchmarks/tiktok
```
