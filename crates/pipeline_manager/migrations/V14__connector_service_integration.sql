-- The connector config field has inside a transport configuration,
-- which can refer to up to two services. These are stored in the
-- added fields alongside the config in separate columns to preserve
-- their reference to the service table. This enables the linked services
-- to be renamed without the connectors having an invalid transport
-- as a result. Upon deserialization, the service names inside the
-- transport configuration are replaced with the names matching the
-- stored identifiers.
--
-- Only two service columns are added and not a separate table.
-- The reason for this is that connectors only need a single service
-- (of the data origin), possibly two (if there is need for a go-between).
-- In the future, if use cases arise, this can be increased or moved to
-- a separate table.
--
-- ON DELETE behavior is CASCADE, which means that if a service is deleted,
-- all of its associated connectors are deleted as well.

ALTER TABLE connector
ADD COLUMN service1 uuid NULL DEFAULT NULL,
ADD COLUMN service2 uuid NULL DEFAULT NULL,
ADD CONSTRAINT service1_service FOREIGN KEY (service1) REFERENCES service(id) ON DELETE CASCADE,
ADD CONSTRAINT service2_service FOREIGN KEY (service2) REFERENCES service(id) ON DELETE CASCADE;
