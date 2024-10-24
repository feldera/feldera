# simple-count

Start a local Kafka instance (reachable at `localhost:19092`):
```
docker compose -f deploy/docker-compose.yml \
               -f deploy/docker-compose-dev.yml \
               -f deploy/docker-compose-extra.yml \
               -f deploy/docker-compose-demo.yml \
               --profile demo-simple-count up --build --renew-anon-volumes --force-recreate
```

Followed by running the demo:
```
cd demo/simple-count
python3 run.py --api-url http://localhost:8080 --kafka localhost:19092
```
