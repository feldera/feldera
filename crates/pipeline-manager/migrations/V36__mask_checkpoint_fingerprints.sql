-- `storage_status_details` embeds each checkpoint's fingerprint as a plain
-- JSON integer. Generated REST clients (e.g. `fda`) parse it as `i64`, so a
-- fingerprint with the high bit set fails to parse (issue #6841). Circuit
-- fingerprints are now masked to the range 0..2^53 when computed or loaded
-- from a checkpoint file, but a pipeline that isn't currently running never
-- gets a fresh status report to pick up that fix, so its last-cached
-- `storage_status_details` can still hold an out-of-range value. Mask any
-- checkpoint fingerprint already stored here the same way.
UPDATE pipeline
SET storage_status_details = fixed.details::text
FROM (
    SELECT
        p.id,
        jsonb_set(
            p.storage_status_details::jsonb,
            '{checkpoints}',
            (
                SELECT jsonb_agg(
                    jsonb_set(
                        checkpoint,
                        '{fingerprint}',
                        to_jsonb((checkpoint ->> 'fingerprint')::numeric % 9007199254740992::numeric)
                    )
                )
                FROM jsonb_array_elements(p.storage_status_details::jsonb -> 'checkpoints') AS checkpoint
            )
        ) AS details
    FROM pipeline AS p
    WHERE p.storage_status_details IS NOT NULL
      AND p.storage_status_details::jsonb ? 'checkpoints'
      AND EXISTS (
          SELECT 1
          FROM jsonb_array_elements(p.storage_status_details::jsonb -> 'checkpoints') AS checkpoint
          WHERE (checkpoint ->> 'fingerprint')::numeric >= 9007199254740992::numeric
      )
) AS fixed
WHERE pipeline.id = fixed.id;
