-- The time when the user record was created. This is the first login, or the
-- time when an administrator pre-provisioned the user. The web console uses it
-- to report a signup. Existing rows keep NULL because their creation time is
-- unknown, and they must not show as new users. For this reason, the default is
-- set after the column is added.
ALTER TABLE app_user ADD COLUMN created_at timestamptz;
ALTER TABLE app_user ALTER COLUMN created_at SET DEFAULT now();
