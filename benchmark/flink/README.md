# Running Nexmark Benchmarks on Flink

It's useful for comparative purposes to be able to run the same
benchmark on DBSP and other systems.  These instructions explain how
to run the [Nexmark benchmarks](https://github.com/nexmark/nexmark) on
Flink in a reproducible manner.  The benchmarks run on a single
physical node using multiple containers using Docker Compose.  One
container runs the Flink jobmanager, and additional containers (by
default 8) run Flink taskmanager.

DBSP also implements the Nexmark benchmarks.  You can run them with 8
cores and 6 event generator threads with:

```
cargo bench --bench nexmark -- --first-event-rate=10000000 --max-events=100000000 --cpu-cores 8  --num-event-generators 6 --source-buffer-size 10000 --input-batch-size 40000 --csv results.csv
```

The following instructions use `docker` and `docker-compose`, since
that's what most people expect, but they were instead tested with
`podman` and `podman-compose`, which are compatible.

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

6. Build a Docker container based on the upstream Flink container that
   also contains Nexmark, and give it the tag `nexmark`:

```
docker build -t nexmark .
```

7. Start the containers:

```
docker-compose -p nexmark up
```

   To reduce output spew, add `-d` (but sometimes that output can be
   informative):

```
podman-compose up -d -p nexmark
```

8. Run tests.  You might want to do this from a separate terminal,
   because the `docker-compose` from the previous step will spew tons
   of distractions (unless you added `-d` above).

   You can run a single test, a comma-separated list, or all of them:

```
podman exec nexmark_jobmanager_1 run-nexmark.sh q0,q1,q2
podman exec nexmark_jobmanager_1 run-nexmark.sh all
```

   You can also run a shell in any of the containers, e.g.:

```
podman exec -it nexmark_jobmanager_1 bash
podman exec -it nexmark_taskmanager_1 bash
```

   If there are failures, then `/opt/flink/log` and `/opt/nexmark/log`
   can be informative.

9. When you're done, stop the containers:

```
docker-compose -p nexmark down
```
