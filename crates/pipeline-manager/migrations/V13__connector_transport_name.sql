-- The connector configuration transport name has been changed to
-- include the _input and _output postfix. Below, the name is changed
-- with the input/output being inferred from either their attachment
-- or the fields being present inside the YAML.

-- url -> url_input
UPDATE connector
SET config = REPLACE(config, 'name: url', 'name: url_input')
WHERE config LIKE '%name: url%';

-- s3 -> s3_input
UPDATE connector
SET config = REPLACE(config, 'name: s3', 'name: s3_input')
    WHERE config LIKE '%name: s3%';

-- file (input) -> file_input
UPDATE connector
SET config = REPLACE(config, 'name: file', 'name: file_input')
WHERE config LIKE '%name: file%'
      AND id IN (
          SELECT ac.connector_id
          FROM attached_connector AS ac
          WHERE ac.is_input = TRUE
      );

-- file (output) -> file_output
UPDATE connector
SET config = REPLACE(config, 'name: file', 'name: file_output')
WHERE config LIKE '%name: file%'
      AND config NOT LIKE '%name: file_input%' -- Exclude any previous file_input matches
      AND id IN (
          SELECT ac.connector_id
          FROM attached_connector AS ac
          WHERE ac.is_input = FALSE
      );

-- kafka (input) -> kafka_input
UPDATE connector
SET config = REPLACE(config, 'name: kafka', 'name: kafka_input')
WHERE config LIKE '%name: kafka%'
      AND (
          config LIKE '%topics:%'
          OR
          id IN (
              SELECT ac.connector_id
              FROM attached_connector AS ac
              WHERE ac.is_input = TRUE
          )
      );

-- kafka (output) -> kafka_output
UPDATE connector
SET config = REPLACE(config, 'name: kafka', 'name: kafka_output')
WHERE config LIKE '%name: kafka%'
      AND config NOT LIKE '%name: kafka_input%' -- Exclude any previous kafka_input matches
      AND (
          config LIKE '%topic:%'
          OR
          id IN (
              SELECT ac.connector_id
              FROM attached_connector AS ac
              WHERE ac.is_input = FALSE
          )
      );
