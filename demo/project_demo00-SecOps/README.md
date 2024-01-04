# Demo: SecOps

## Getting started

1. Bring up a Feldera instance, for example reachable at `http://localhost:8080`.

2. Bring up a Redpanda instance, for example reachable by both
   script and Feldera instance at `redpanda:9092`

3. Set environment variable for the script: `export REDPANDA_BROKERS=redpanda:9092`

4. Run the following:
   ```
   cd demo/project_demo00-SecOps
   python3 run.py --api-url="pipeline-manager:8080" --num-pipelines=200000
   ```
   
5. Note that the pipeline does not start automatically, but requires
   to be manually started via the web console or by issuing:
   ```
   curl -X POST http://localhost:8080/v0/pipelines/demo-sec-ops-pipeline/shutdown
   curl -X POST http://localhost:8080/v0/pipelines/demo-sec-ops-pipeline/start
   ```
   
6. Progress can be seen via the web console or by issuing:
   ```
   curl -X GET http://localhost:8080/v0/pipelines/demo-sec-ops-pipeline/stats
   ```

## Usage

```
python3 run.py --help
```
