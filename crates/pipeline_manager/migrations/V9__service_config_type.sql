-- Note: the name of a service can now be changed and as such
--       is no longer regarded as immutable

-- To accommodate the addition of a new column, any pre-existing
-- services are deleted. This is possible as services are
-- experimental and not yet used by the rest of the system.
-- Some might have been created via the services API which
-- was already added for testing. Services were not yet
-- added to the UI.
DELETE FROM service;

-- ServiceConfig enumeration variant type (e.g., "kafka")
ALTER TABLE service
ADD COLUMN config_type varchar NOT NULL;
