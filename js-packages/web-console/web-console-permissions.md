# web-console permissions and UI gating plan

Plan for gating interactive UI on the caller's role. Builds on the role model in
`AUTH_AND_TENANCY.md`; read that for where the role comes from
(`page.data.feldera.role`).

## 1. Model

web-console gates on named permissions, not on the role directly. A permission is
`verb:resource`. Three verbs, kept deliberately few:

| Verb    | Meaning                                                            |
| ------- | ------------------------------------------------------------------ |
| `read`  | view a resource                                                    |
| `write` | change a resource's definition or data (create, edit, delete)      |
| `exec`  | run an operation against a running pipeline (no definition change) |

`delete` is `write` for now.

The backend still enforces a single ordered role (`read < write < admin < owner`)
and gates each route at one minimum role. web-console does not re-derive that by
rank. Instead it keeps its own hardcoded role→permission map (§3). The map happens
to mirror the backend today, but stating it explicitly means a gate reads as "this
feature needs `exec:runtime_upgrade`", and the role that grants it is one lookup,
not a rank comparison scattered across call sites.

## 2. Permission catalog

| Permission              | Feature group                                                                 |
| ----------------------- | ----------------------------------------------------------------------------- |
| `read:pipeline`         | list/view pipelines, status, stats, logs, metrics, dataflow graph             |
| `read:pipeline_code`    | view SQL / UDF code                                                           |
| `read:pipeline_config`  | view runtime / program config, resources                                      |
| `read:support_bundle`   | download support bundle, collect heap/samply/circuit profiles, diff           |
| `read:cluster_health`   | cluster monitor events, the health page, the header's health indicator        |
| `write:pipeline`        | create, duplicate, import demo, delete                                        |
| `write:pipeline_code`   | edit SQL / UDF Rust / UDF TOML                                                |
| `write:pipeline_config` | edit runtime config, compilation profile, resources                           |
| `write:pipeline_meta`   | rename, tags                                                                  |
| `exec:pipeline`         | start/stop/pause/resume/standby/activate, kill, clear, approve, dismiss error |
| `exec:checkpoint`       | checkpoint now, sync to object store                                          |
| `exec:runtime_upgrade`  | recompile / update runtime version                                            |
| `exec:pipeline_data`    | ad-hoc SQL, data ingress                                                      |
| `write:api_key`         | list / create / delete API keys                                               |
| `write:tenant_member`   | list / add / set-role / remove tenant members                                 |
| `write:oidc_trust`      | per-tenant OIDC trust CRUD                                                    |
| `write:tenant`          | list / create / rename / delete tenants                                       |
| `write:owner_trust`     | platform-wide owner OIDC trust CRUD                                           |

## 3. Role → permission map (hardcoded, cumulative)

Each role grants everything the previous role grants, plus the rows below. This is
the source of truth in `src/lib/services/rbac.ts` (§5.1).

| Role    | Adds                                                                                                                                                                                       |
| ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `read`  | `read:pipeline`, `read:pipeline_code`, `read:pipeline_config`, `read:support_bundle`, `read:cluster_health`                                                                                |
| `write` | `write:pipeline`, `write:pipeline_code`, `write:pipeline_config`, `write:pipeline_meta`, `exec:pipeline`, `exec:checkpoint`, `exec:runtime_upgrade`, `exec:pipeline_data`, `write:api_key` |
| `admin` | `write:tenant_member`, `write:oidc_trust`                                                                                                                                                  |
| `owner` | `write:tenant`, `write:owner_trust`                                                                                                                                                        |

Consequences for `read`:

- Sees every pipeline, its code, config, stats, logs. Reads are the floor within a
  tenant, so they are gated only where a session without one could reach them.
- Downloads support bundles and collects profiling data (all `read`-role on the backend).
- No pipeline actions, no editing, no ad-hoc query, no API keys, no admin.

## 4. Features to gate, by file

Style: `hide` renders only when permitted; `disable` keeps the control inert with
a read-only hint; `readonly` puts an editor in read-only mode. Read features are
omitted here; they are ungated.

### 4.1 Pipeline lifecycle and creation

| File                                                                                                | Control                                                                          | Permission                                           | Style |
| --------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | ---------------------------------------------------- | ----- |
| `pipelines/PipelineNameInput.svelte`, `pipelines/CreatePipelineButton.svelte`                       | name input + New Pipeline                                                        | `write:pipeline`                                     | hide  |
| `pipelines/list/Actions.svelte`                                                                     | start / start-paused / resume / standby / activate / pause / stop / kill / clear | `exec:pipeline`                                      | hide  |
| `pipelines/list/Actions.svelte`                                                                     | More menu: delete, duplicate                                                     | `write:pipeline`                                     | hide  |
| `pipelines/table/AvailableActions.svelte`                                                           | bulk start/resume/pause/stop/kill/clear; delete; duplicate                       | `exec:pipeline` (delete/duplicate: `write:pipeline`) | hide  |
| `pipelines/editor/performance/{CheckpointsIndicator,CheckpointsStatus,CheckpointDialog}.svelte`     | create checkpoint                                                                | `exec:checkpoint`                                    | hide  |
| `layout/pipelines/PipelineEditLayout.svelte`, `pipelines/editor/ReviewPipelineChangesDialog.svelte` | dismiss deployment error; approve changes                                        | `exec:pipeline`                                      | hide  |

Preference applied: pipeline actions are hidden for `read`, not shown-disabled.
Existing disable reasons inside these controls (Enterprise-only stop via
`usePremiumFeatures`, unsaved-changes) stay; they run only for `write`+ who now
see the control at all.

### 4.2 Editing

| File                                                                              | Control                                             | Permission              | Style                 |
| --------------------------------------------------------------------------------- | --------------------------------------------------- | ----------------------- | --------------------- |
| `layout/pipelines/PipelineCodePanel.svelte`, `pipelines/editor/CodeEditor.svelte` | Monaco SQL / UDF editors, save, conflict-resolution | `write:pipeline_code`   | readonly              |
| `pipelines/list/Actions.svelte` (`_saveFile`)                                     | save-file button / "File saved" indicator           | `write:pipeline_code`   | hide                  |
| `layout/pipelines/PipelineConfigurationsPopup.svelte`                             | runtime / compilation config JSON editors, Apply    | `write:pipeline_config` | readonly + hide Apply |
| `layout/pipelines/PipelineEditLayout.svelte` (`DoubleClickInput`)                 | rename pipeline                                     | `write:pipeline_meta`   | hide                  |
| `pipelines/table/Tags.svelte`                                                     | assign/unassign, create / edit / delete tag         | `write:pipeline_meta`   | hide                  |

`EditorOptionsPopup.svelte` (autosave, minimap, font size) is a local preference,
not gated.

### 4.3 Data and runtime

| File                                                                                          | Control                                | Permission             | Style    |
| --------------------------------------------------------------------------------------------- | -------------------------------------- | ---------------------- | -------- |
| `pipelines/editor/MonitoringPanel.svelte`, `pipelines/editor/InteractionPanel.svelte`         | Ad-Hoc Queries and Changes Stream tabs | `exec:pipeline_data`   | hide tab |
| `pipelines/editor/StorageInUseBanner.svelte`, `pipelines/table/PipelineVersionTooltip.svelte` | Update runtime version                 | `exec:runtime_upgrade` | hide     |

The Ad-Hoc Queries and Changes Stream tabs both read/stream live pipeline data,
so the whole tab is hidden rather than made read-only. Each panel drops a saved
tab selection that points at a now-hidden tab back to the first visible tab on
init, so a `read` caller never lands on a blank panel. Because the tab is hidden,
`TabAdHocQuery.svelte` / `adhoc/Query.svelte` carry no in-tab gate of their own.

### 4.4 Demos (conditional for `read`)

| File                                                                                                                                   | Behavior                                                                                                                                                                                                       |
| -------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `other/DemoTile.svelte`, `compositions/pipelines/useTryPipeline.ts`, `routes/(system)/(authenticated)/(authorized)/(shell)/demos/+page.svelte` | Enabled when a pipeline with the demo's name already exists (clicking navigates to it, allowed for `read`) OR the caller has `write:pipeline` (clicking creates it). Disable the tile only when neither holds. |

This is the one gate that is not a plain permission check: `enabled = pipelineExists || has('write:pipeline')`. The header Create Pipeline button stays a plain `write:pipeline` hide.

### 4.5 API keys and admin (mostly shipped)

| File                                                                                            | Permission            | Min role | Status                                                                                           |
| ----------------------------------------------------------------------------------------------- | --------------------- | -------- | ------------------------------------------------------------------------------------------------ |
| `auth/ProfileButton.svelte`, `other/ApiKeyMenu.svelte`, `apiKey/NewApiKeyForm.svelte`           | `write:api_key`       | write    | menu already gated; migrate its ad-hoc role check to `write:api_key`                             |
| `routes/(system)/(authenticated)/(authorized)/(shell)/admin/+page.ts`                                   | `write:tenant_member` | admin    | already redirects; express via permission                                                        |
| `admin/UserRoleTable.svelte`                                                                    | `write:tenant_member` | admin    | in admin area                                                                                    |
| `other/OidcTrustMenu.svelte`, `oidcTrust/NewOidcTrustForm.svelte`                               | `write:oidc_trust`    | admin    | in gated menu                                                                                    |
| `admin/TenantList.svelte`, `admin/AdminPage.svelte` (tenant switcher, Platform owners, Tenants) | `write:tenant`        | owner    | gated on `write:tenant`; owner-trust CRUD removed (owner is deploy-time config, shown read-only) |
| `auth/ProfileButton.svelte` (Feldera Health entry)                                              | `read:cluster_health` | read     | the one read gate: this header also renders on `/select-tenant`, where a session holds nothing   |

Within `NewApiKeyForm.svelte`, both `read` and `write` key options stay offered:
everyone who can open the menu already holds `write:api_key`, so the old
`canGrantWrite` branch collapses.

### 4.6 Not gated (account / session / UI-local)

`auth/CurrentTenant.svelte` (tenant switch), `auth/ProfileButton.svelte` Sign Out,
`other/AuthErrorToast.svelte` (re-auth), `layout/userPopup/DarkModeSwitch.svelte`,
`layout/pipelines/EditorOptionsPopup.svelte`, `layout/{Drawer,InlineDrawer}.svelte`,
`profile-viewer/+page.svelte`, `version/**`. The `/health` page itself carries no
gate: it sits in the `(authorized)` route group, so reaching it already means a
tenant resolved, and every role holds `read:cluster_health`. The header entry
linking to it is gated, since that header also renders on `/select-tenant`.

## 5. Technical design

No `canWrite` / `isOwner` booleans. Every gate names the permission it needs; the
role→permission map is the only place roles appear.

### 5.1 `rbac.ts`: the map and the check

Pure module, no runes, unit-testable.

`src/lib/services/rbac.ts`

```ts
export type Role = 'read' | 'write' | 'admin' | 'owner'

export type Permission =
  | 'read:pipeline'
  | 'read:pipeline_code'
  | 'read:pipeline_config'
  | 'read:support_bundle'
  | 'read:cluster_health'
  | 'write:pipeline'
  | 'write:pipeline_code'
  | 'write:pipeline_config'
  | 'write:pipeline_meta'
  | 'exec:pipeline'
  | 'exec:checkpoint'
  | 'exec:runtime_upgrade'
  | 'exec:pipeline_data'
  | 'write:api_key'
  | 'write:tenant_member'
  | 'write:oidc_trust'
  | 'write:tenant'
  | 'write:owner_trust'

// Ordered low to high. Each role adds to the ones before it.
const ROLES: Role[] = ['read', 'write', 'admin', 'owner']

const GRANTS: Record<Role, Permission[]> = {
  read: [
    'read:pipeline',
    'read:pipeline_code',
    'read:pipeline_config',
    'read:support_bundle',
    'read:cluster_health'
  ],
  write: [
    'write:pipeline',
    'write:pipeline_code',
    'write:pipeline_config',
    'write:pipeline_meta',
    'exec:pipeline',
    'exec:checkpoint',
    'exec:runtime_upgrade',
    'exec:pipeline_data',
    'write:api_key'
  ],
  admin: ['write:tenant_member', 'write:oidc_trust'],
  owner: ['write:tenant', 'write:owner_trust']
}

// Precompute the cumulative set per role once.
const PERMISSIONS: Record<Role, ReadonlySet<Permission>> = (() => {
  const acc: Permission[] = []
  const out = {} as Record<Role, Set<Permission>>
  for (const role of ROLES) {
    acc.push(...GRANTS[role])
    out[role] = new Set(acc)
  }
  return out
})()

export const hasPermission = (role: Role, permission: Permission): boolean =>
  PERMISSIONS[role].has(permission)

// What a session reports when it holds no role. A named case rather than
// `undefined`, so `page.data.feldera.role` is total and a consumer that forgets
// this case gets a type error. `Role` stays the four roles the backend grants.
export const NO_ROLE = 'no_role'
export type SessionRole = Role | typeof NO_ROLE

// The permissions a role grants, and none for NO_ROLE. `+layout.ts` materializes
// this into `page.data.feldera.permissions` at session-config init.
export const permissionsOf = (role: SessionRole): Permission[] =>
  role === NO_ROLE ? [] : [...PERMISSIONS[role]]

// The role a session holds, or NO_ROLE. The server sends `role: null` exactly
// when no acting tenant resolved, since a role is granted per membership. An
// unrecognized role reads the same way, so a backend role the map has not caught
// up with grants nothing.
export const roleOf = (role: string | null | undefined): SessionRole =>
  (ROLES as string[]).includes(role ?? '') ? (role as Role) : NO_ROLE

export const NO_PERMISSIONS: readonly Permission[] = Object.freeze([])

// Session-facing check: reads the materialized list off `page.data.feldera`. An
// absent `feldera` means no session resolved a tenant, and grants nothing, so
// gates deny by default the way the backend's route table does.
export const hasPermissions = (
  feldera: { permissions: readonly Permission[] } | undefined,
  permission: Permission
): boolean => (feldera?.permissions ?? NO_PERMISSIONS).includes(permission)
```

A `NO_ROLE` session holding no permissions is what lets every gate be a plain
`<RBAC>`. The alternative, reporting `read` for a missing role, would make read
gates useless: the header renders on `/select-tenant`, where a read-role
permission would be held by a session that cannot call a single tenant-scoped
route.

`+layout.ts` normalizes the role once with `roleOf` and injects the granted
permissions, so the role to permission map is applied at the single boundary
where session data enters, and every gate reads a data field:

```ts
const role = roleOf(sessionConfig?.role)
// ...page.data.feldera:
role,
permissions: permissionsOf(role)
```

### 5.2 `<RBAC>`: the gating wrapper

One component covers the common markup gates. It reads the role reactively from
`page` (the Svelte 5 replacement for `let:` is the snippet parameter, so the child
snippet receives an `RBACState`).

`src/lib/components/auth/RBAC.svelte`

```svelte
<script lang="ts" module>
  export type RBACState = {
    allowed: boolean
    // Spread onto the gated element to apply the read-only look when disallowed;
    // empty object when allowed.
    disabledProps: Record<string, unknown>
  }
</script>

<script lang="ts">
  import type { Snippet } from 'svelte'
  import { page } from '$app/state'
  import { hasPermissions, type Permission } from '$lib/services/rbac'

  let {
    require: permission,
    mode = 'hide',
    message = 'You have read-only access',
    children
  }: {
    require: Permission
    mode?: 'hide' | 'disable'
    message?: string
    children: Snippet<[RBACState]>
  } = $props()

  const allowed = $derived(hasPermissions(page.data.feldera, permission))
  const state = $derived<RBACState>({
    allowed,
    disabledProps: allowed
      ? {}
      : {
          disabled: true,
          'aria-disabled': 'true',
          title: message,
          class: 'pointer-events-none opacity-50'
        }
  })
</script>

{#if mode === 'disable' || allowed}
  {@render children(state)}
{/if}
```

Three ways to use it, in ascending manual control:

```svelte
<!-- hide: control vanishes for those without the permission -->
<RBAC require="write:pipeline">
  <CreatePipelineButton />
</RBAC>

<!-- disable: control stays, spread the ready-made attrs+class -->
<RBAC require="exec:runtime_upgrade" mode="disable">
  {#snippet children({ disabledProps })}
    <button {...disabledProps} onclick={update}>Update</button>
  {/snippet}
</RBAC>

<!-- manual: use the boolean yourself for anything the spread can't express -->
<RBAC require="write:pipeline_meta" mode="disable">
  {#snippet children({ allowed })}
    <TagChip editable={allowed} />
  {/snippet}
</RBAC>
```

`disabledProps` carries the disabled attribute, `aria-disabled`, a native `title`
tooltip and the read-only Tailwind classes, so the look and the reason stay
identical everywhere without each call site restating them. Swap `title` for a
common-ui `Popover` if a richer hint is wanted; keep it inside `RBAC` so it stays
consistent.

### 5.3 `usePermission`: for non-markup gates

Some gates feed a boolean into existing plumbing rather than wrapping markup, e.g.
Monaco's `editDisabled`. A one-permission composition serves those without a
generic capability flag.

`src/lib/compositions/usePermission.svelte.ts`

```ts
import { page } from '$app/state'
import { hasPermissions, type Permission } from '$lib/services/rbac'

export const usePermission = (permission: Permission) => {
  const allowed = $derived(hasPermissions(page.data.feldera, permission))
  return {
    get allowed() {
      return allowed
    }
  }
}
```

The code editor already OR-chains its disable reasons at
`layout/pipelines/PipelineCodePanel.svelte:56` and passes `editDisabled` into
`pipelines/editor/CodeEditor.svelte`; add the permission as one more term:

```ts
const codeEdit = usePermission('write:pipeline_code')
let editCodeDisabled = $derived(
  !codeEdit.allowed || !pipeline.current || deleted || /* status, upgrade */ ...
)
```

Monaco already shows a `readOnlyMessage` (see `CodeEditor.svelte:383`); route the
read-only reason through it.

### 5.4 Where common-ui fits

`page.data.feldera` is web-console-only, so `rbac.ts`, `RBAC.svelte` and
`usePermission` live in web-console. common-ui supplies presentation only:
`Tooltip`/`Popover` for a richer read-only hint, and `MonacoEditor`'s `readOnly`
option, already threaded through `CodeEditor.svelte`. Lift only presentation into
common-ui if reused; never the role logic.

### 5.5 Conventions

- Name the permission at the gate; never branch on the role or a `canX` flag.
- Gate the affordance, not just the request, so a `read` user never hits a 403.
- Compose, do not replace, existing disable predicates (premium, unsaved, status).
- Server stays authoritative; UI gating is UX.

### 5.6 Testing

Per `web-console-test-file-conventions`, `.spec.ts` unit tests mount each gated
component under a mocked `page.data.feldera.role` and assert hidden / `disabled` /
read-only across `read`, `write`, and `admin`/`owner` where relevant. Unit-test
`hasPermission` directly against the map. Confirm a gate's test catches regressions
by removing the gate and watching it fail.
