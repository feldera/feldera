# Running Nexmark Benchmarks on Flink with Kafka

It's useful to be able to run the same benchmark on Feldera and other
systems.  These instructions explain how to run the [Nexmark
benchmarks](https://github.com/nexmark/nexmark) on Flink in a
reproducible manner.  The benchmarks run on a single physical node
using multiple containers using Docker Compose.  One container runs
the Flink jobmanager, and additional containers (by default 8) run
Flink taskmanager.

These instructions run Flink with Kafka as a data source.  **These
instructions are unsatisfactory because they will use all your disk
space quickly (1+ TB in 30 minutes).  Presumably that is fixable but
it is not yet fixed.**

1. `cd` to the directory that contains this `README.md`, because the
   files in this directory are useful for building the Docker
   container and running Docker Compose.

2. Download and unpack the Nexmark benchmark:

```
git clone https://github.com/nexmark/nexmark.git
```

3. Apply a couple of patches:

```
(cd nexmark && patch -p1 < ../avoid-jps-dependency.patch)
(cd nexmark && patch -p1 < ../omit-q10.patch)
```

4. Build Nexmark:

```
(cd nexmark/nexmark-flink && ./build.sh)
```

   If it doesn't build, it might mean you've got the wrong version of
   the JDK installed.  Nexmark and Flink only work with JDK 1.8 and
   1.11, not newer versions.

   This should output `nexmark/nexmark-flink/nexmark-flink.tgz`.  Be
   warned that `build.sh` doesn't bail out on errors, so you might
   still get something there even if the build partially fails.

5. Extract Nexmark archive:

```
tar xzf nexmark/nexmark-flink/nexmark-flink.tgz
```

6. Download and unpack Kafka:

```
wget https://dlcdn.apache.org/kafka/3.4.0/kafka_2.13-3.4.0.tgz
tar xfz kafka_2.13-3.4.0.tgz
```

7. Download the Flink connector for Kafka:

```
wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.0/flink-sql-connector-kafka-1.17.0.jar
```

8. Build a Docker container based on the upstream Flink container that
   also contains Nexmark and Kafka, and give it the tag
   `nexmark-kafka`:

```
docker build -t nexmark-kafka .
```

7. Start the containers:

```
docker compose -p nexmark up
```

   To reduce output spew, add `-d` (but sometimes that output can be
   informative):

```
docker compose up -d -p nexmark
```

8. Run the `insert_kafka` query to start inserting data into Kafka.
   This query will continue running in the background even after it
   "finishes".  It will consume your disk space at an incredible rate
   (over 1 TB in 30 minutes).
   
   You might want to do this from a separate terminal, because the
   `docker compose` from the previous step will spew tons of
   distractions (unless you added `-d` above).

```
docker exec nexmark-jobmanager-1 run-nexmark.sh insert_kafka
```

9. Run tests.

   You can run a single test, a comma-separated list, or all of them:

```
docker exec nexmark-jobmanager-1 run-nexmark.sh q0,q1,q2
docker exec nexmark-jobmanager-1 run-nexmark.sh all
```

   You can also run a shell in any of the containers, e.g.:

```
docker exec -it nexmark-jobmanager-1 bash
docker exec -it nexmark-taskmanager-1 bash
```

   You'll probably run out of disk space if you run more than one.

   If there are failures, then `/opt/flink/log` and `/opt/nexmark/log`
   can be informative.

10. When you're done, stop the containers:

```
docker compose -p nexmark down
```
