CREATE TABLE crm_profiles (
  profile_id BIGINT,
  email VARCHAR,
  phone VARCHAR,
  country VARCHAR
);

CREATE TABLE identity_events (
  event_id BIGINT,
  email VARCHAR,
  phone VARCHAR,
  observed_at TIMESTAMP
);

CREATE VIEW profile_identity_matches AS
SELECT
  p.profile_id,
  e.event_id,
  p.country
FROM crm_profiles p
JOIN identity_events e
  ON p.email <=> e.email
 AND p.phone <=> e.phone;
