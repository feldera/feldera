-- Provenance for tenant memberships: when a row was created and which path
-- created it. `origin` is 'claim' (the user's token listed the tenant),
-- 'derived' (the personal or issuer tenant derivation), or 'api' (granted
-- through the RBAC endpoints). Rows from before this migration keep NULL in
-- both columns: their provenance is unknown.
ALTER TABLE tenant_membership ADD COLUMN created_at timestamptz;
ALTER TABLE tenant_membership ADD COLUMN origin varchar
    CHECK (origin IN ('claim', 'derived', 'api'));

-- Profile fields the identity provider owns, refreshed from its OIDC UserInfo
-- endpoint. `app_user.email` already existed but carried no indication of where
-- it came from: an access-token claim, an administrator typing it into the
-- pre-provisioning API, or the provider itself. `email_verified` supplies that
-- distinction, so an interface can mark an address the provider vouches for and
-- leave every other one unmarked.
--
-- Verification is false until a provider says otherwise, so that a provider
-- saying nothing never reads as an endorsement. The rest stay NULL until the
-- first refresh runs, and for providers publishing no `userinfo_endpoint`.
ALTER TABLE app_user ADD COLUMN email_verified boolean NOT NULL DEFAULT false;
ALTER TABLE app_user ADD COLUMN display_name varchar;

-- When the last refresh attempt ran, successful or not, so a provider that
-- fails or answers with nothing is retried on a schedule rather than on every
-- request.
ALTER TABLE app_user ADD COLUMN profile_refreshed_at timestamptz;

-- The `auth_time` of the token that last attempt covered. A token carrying a
-- newer one means the user authenticated again, which is the moment a changed
-- email can appear, so it forces a refresh ahead of the schedule.
ALTER TABLE app_user ADD COLUMN profile_auth_time bigint;
