-- TODO: migration of existing services and connectors?

-- The ConnectorConfig can refer up to two services, which are stored in these
-- fields. These are added because the ConnectorConfig as an API object actually
-- uses names rather than identifiers to refer to services. Upon deserialization,
-- ConnectorConfig is constructed with the names matching these stored identifiers.
-- This allows changing of names while the connector is stored in the database.
--
-- Q: Why two and why is it not in a separate table?
-- A: Currently, connectors only need a single service (of the data origin),
--    possibly two (if there is need for a go-between). In the future, if
--    use cases arise, this can be increased or moved to a separate table.
--
-- ON DELETE behavior is CASCADE, which means that if a service is deleted,
-- all of its associated connectors are deleted as well.

ALTER TABLE connector
ADD COLUMN service1 uuid NULL DEFAULT NULL,
ADD COLUMN service2 uuid NULL DEFAULT NULL,
ADD CONSTRAINT service1_service FOREIGN KEY (service1) REFERENCES service(id) ON DELETE CASCADE,
ADD CONSTRAINT service2_service FOREIGN KEY (service2) REFERENCES service(id) ON DELETE CASCADE;
