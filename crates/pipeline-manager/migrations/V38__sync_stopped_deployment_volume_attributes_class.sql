-- A `PATCH` of `deployment_config` can now change the volume attributes class, so a start takes it
-- from the previous `deployment_config` (until storage is cleared), as V37 does for CPU and memory.
--
-- Before, a start took it from `runtime_config`, so a stopped pipeline edited since its last start
-- has a stale class in `deployment_config`. Copy it over so the edit is not lost. Storage needs no
-- sync: it can only be edited with storage cleared, and such a start takes it from
-- `runtime_config`.
UPDATE pipeline
SET deployment_config = jsonb_set(
        deployment_config::jsonb,
        '{resources}',
        (COALESCE(deployment_config::jsonb -> 'resources', '{}') - 'volume_attributes_class')
        || COALESCE(
            (SELECT jsonb_build_object('volume_attributes_class', class)
             FROM (SELECT CASE jsonb_typeof(runtime_config::jsonb -> 'resources')
                              WHEN 'object' THEN runtime_config::jsonb -> 'resources'
                                                 -> 'volume_attributes_class'
                          END AS class) AS runtime
             WHERE class IS NOT NULL AND class <> 'null'::jsonb),
            '{}'))::varchar
WHERE deployment_resources_status = 'stopped'
  AND deployment_config IS NOT NULL;
