# Running Nexmark Benchmarks on Flink

It's useful for comparative purposes to be able to run the same
benchmark on DBSP and other systems.  These instructions explain how
to run the [Nexmark benchmarks](https://github.com/nexmark/nexmark) on
Flink in a reproducible manner.  The benchmarks run on a single
physical node using multiple containers using Docker Compose.  One
container runs the Flink jobmanager, and additional containers run
Flink taskmanager.

By default, these benchmarks run 8 replicas of the Flink taskmanager,
with 2 threads each.  Edit `docker-compose.yml` to adjust this
configuration.

The following instructions use `docker` and `docker-compose`, since
that's what most people expect, but they were instead tested with
`podman` and `podman-compose`, which are compatible.

## Building Nexmark

You can follow the instructions below to build Nexmark, or run
`build.sh` in this directory.

1. `cd` to the directory that contains this `README.md`, because the
   files in this directory are useful for building the Docker
   container and running Docker Compose.

2. Clone a fork of the Nexmark benchmark, e.g.:

   ```
   git clone git@github.com:blp/nexmark.git
   ```

   This fork has a few commits that are necessary for running Nexmark
   under Docker.

4. Build Nexmark:

   ```
   (cd nexmark/nexmark-flink && ./build.sh)
   ```

   This should output `nexmark/nexmark-flink/nexmark-flink.tgz`.  If
   it doesn't build, it might mean you've got the wrong version of the
   JDK installed.  Nexmark and Flink only work with JDK 1.8 and 1.11,
   not newer versions.

   The build will take a minute or so.  It will output some warnings.
   They shouldn't keep it from finishing with a `BUILD SUCCESS`
   message.

5. Extract the Nexmark archive from the previous step:

   ```
   tar xzf nexmark/nexmark-flink/nexmark-flink.tgz
   ```

6. Build a Docker container based on the upstream Flink container that
   also contains Nexmark, and give it the tag `nexmark`:

   ```
   docker build -t nexmark .
   ```

   This should take less than a minute (if it needs to download base
   layers, that could take longer).  It should finish with the message
   `Successfully tagged localhost/nexmark:latest` and a UUID.

## Running Nexmark

You can use `run-nexmark.sh` in the parent directory to do all of the
following, or follow these instructions to do them by hand:

1. Start the containers:

   ```
   docker-compose -p nexmark up
   ```

   To reduce output spew, add `-d` (but sometimes that output can be
   informative):

   ```
   docker-compose -p nexmark up -d
   ```

2. Run tests.  You might want to do this from a separate terminal,
   because the `docker-compose` from the previous step will spew tons
   of distractions (unless you added `-d` above).

   You can use `--queries` and `--events` to control how the test
   runs:

   ```
   docker exec nexmark_jobmanager_1 run.sh
   docker exec nexmark_jobmanager_1 run.sh --queries q0,q1,q2
   docker exec nexmark_jobmanager_1 run.sh --events 1000000
   docker exec nexmark_jobmanager_1 run.sh --help
   ```

   You can also run a shell in any of the containers, e.g.:

   ```
   docker exec -it nexmark_jobmanager_1 bash
   docker exec -it nexmark_taskmanager_1 bash
   ```

   If there are failures, then `/opt/flink/log` and `/opt/nexmark/log`
   can be informative.

3. When you're done, stop the containers:

   ```
   docker-compose -p nexmark down -t 0
   ```
