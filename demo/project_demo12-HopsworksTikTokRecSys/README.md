/ Hopsworks Tiktok Recommendation System Demo

### Getting Started

0. Ensure that the feldera python API is installed.

```sh
pip install feldera/ # from the base directory of this repo
```

1. Run feldera with redpanda

```sh
cargo run --bin=pipeline-manager
docker run --name redpanda -p 9092:9092 --rm -itd docker.redpanda.com/vectorized/redpanda:v24.2.4 redpanda start --smp 2
export KAFKA_SERVER=localhost:9092
export KAFKA_SERVER_FROM_PIPELINE=$KAFKA_SERVER
```

Or,

```sh
docker compose -f deploy/docker-compose.yml \
               -f deploy/docker-compose-dev.yml \
               --profile redpanda up --build --renew-anon-volumes --force-recreate
```

2. Running the generator

For the Rust generator:

```sh
export MAX_EVENTS=1000000
cd tiktok-gen
cargo run --release -- --historical --delete-topic-if-exists -I $MAX_EVENTS -B $KAFKA_SERVER
DATA_FMT=csv
```

For the python generator:

```sh
python 0_prepare_data.py
```

3. Run the pipeline

```sh
python 1_pipeline.py
```



### Generators
This demo has two generators:
1. The python version that is the original generator.
2. The rust version (`tiktok-gen`) that is much faster but the distribution of
   the data generated may be different.
