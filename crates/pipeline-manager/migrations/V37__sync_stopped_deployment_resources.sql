-- `PATCH /v0/pipelines/{name}/deployment` resizes a running pipeline by changing the CPU and
-- memory in its `deployment_config`. To keep a resize across stop and start, a start now takes
-- CPU and memory from the previous `deployment_config` (until storage is cleared), and a CPU or
-- memory edit in `runtime_config` is copied into `deployment_config`.
--
-- Before, a start took them from `runtime_config`, so a stopped pipeline edited since its last
-- start has stale values in `deployment_config`. Copy them over so the edit is not lost.
UPDATE pipeline
SET deployment_config = jsonb_set(jsonb_set(jsonb_set(jsonb_set(
        deployment_config::jsonb,
        '{resources,cpu_cores_min}', COALESCE(runtime_config::jsonb #> '{resources,cpu_cores_min}', 'null')),
        '{resources,cpu_cores_max}', COALESCE(runtime_config::jsonb #> '{resources,cpu_cores_max}', 'null')),
        '{resources,memory_mb_min}', COALESCE(runtime_config::jsonb #> '{resources,memory_mb_min}', 'null')),
        '{resources,memory_mb_max}', COALESCE(runtime_config::jsonb #> '{resources,memory_mb_max}', 'null'))::varchar
WHERE deployment_resources_status = 'stopped'
  AND deployment_config IS NOT NULL;
