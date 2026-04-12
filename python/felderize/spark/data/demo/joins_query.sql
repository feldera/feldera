CREATE OR REPLACE TEMP VIEW profile_identity_matches AS
SELECT
  p.profile_id,
  e.event_id,
  p.country
FROM crm_profiles p
JOIN identity_events e
  ON p.email <=> e.email
 AND p.phone <=> e.phone;
