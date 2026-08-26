# f5a

A k9s-style terminal console for Feldera: watch, drive, and profile pipelines
from one keyboard-driven screen. The header hints, `?` help, and `:samply`
capture badges all live on the dashboard, so no tab is needed for profiling.

```
Instance: http://127.0.0.1:8080   <enter> metrics    <f> hotspots    ███████╗███████╗ █████╗
...
╭ PIPELINES [1]  sort:name↑ ──────────────────────────────────────────────────────────────────╮
│● f5a_demo   Running   Success   5.0K/s  ▁▇█▇   802.5K   0   111.7 MiB   119 B   0    2m    │
```

## Run

```bash
cargo run -p f5a -- --host http://127.0.0.1:8080
```

| Flag | Environment | Default | Meaning |
|------|-------------|---------|---------|
| `--host` | `FELDERA_HOST` | `http://127.0.0.1:8080` | API endpoint |
| `--api-key` | `FELDERA_API_KEY` | none | Bearer token (refused over plaintext HTTP to remote hosts) |
| `--tenant` | `FELDERA_TENANT` | none | Acting tenant on multi-tenant instances |
| `-k`, `--insecure` | `FELDERA_TLS_INSECURE` | off | Accept invalid TLS certificates |
| `--timeout-secs` | | 15 | Per-request timeout |
| `--refresh-secs` | | 2 | Poll cadence (runtime: `:refresh-every N`) |
| `--ask` | | off | Confirm destructive actions (stop, force-stop, restart, clear) before running them |
| `--auth-command` | `FELDERA_AUTH_COMMAND` | none | Shell command printing a bearer token; re-run as tokens age (mutually exclusive with `--api-key`) |
| `--download-dir` | `FELDERA_DOWNLOAD_DIR` | the OS Downloads folder | Where downloads such as Samply profiles land; created on first use |

## Views

| Key | View | Content |
|-----|------|---------|
| `1` | Pipelines | Live table: status (animated while transitioning, with the target state), tags, runtime version, rec/s, activity sparkline, records, buffer, memory, storage, connector errors, age |
| `2` | Metrics | Gauges, throughput and memory charts from `/time_series`, every global metric |
| `3` | Connectors | Per-endpoint counters; `enter` inspects the raw JSON; `p`/`P` pause/resume inputs |
| `4` | Hotspots | Circuit operators ranked by time, memory, or state records (`m` cycles) |
| `5` | SQL | Highlighted program with per-line profiler heat; `enter` on a hotspot jumps here |
| `6` | Logs (`l`) | Compile and deployment errors (Rust stderr tail, SQL messages) above the live log stream, with follow mode; `enter` on a broken stopped pipeline lands here |

Global keys: `tab`/`1..6` or a click on the bottom tab bar switch views,
`j`/`k` move and stop at the ends (the mouse wheel scrolls, a click selects
the row), `/` filter, `:` command bar (tab
completion, history), `?` help, `r` refresh, `T` tenant picker, `l` logs,
`q`/`ctrl-c` quit. Lifecycle: `s` start, `p`/`P` pause/resume, `x`/`X`
stop/force-stop, `C` clear. Destructive actions run immediately; pass
`--ask` to confirm them first.

The table scrolls with the selection and shows a scrollbar when the list
outgrows the terminal; `pgup`/`pgdn` move by a full page. Clicking a column
header sorts by it, ascending first and toggling direction on repeat clicks
(the active header wears a direction arrow). The memory column escalates
with the engine's reported memory pressure: `▲` moderate, `♨` high, `🔥`
critical (pressure is computed against a configured `max_rss`). The storage
column escalates the same way against `resources.storage_mb_max` when one
is configured, using the same 85/90/95% thresholds; the quota arrives with
the status payload (`runtime_config.resources.storage_mb_max`), so no extra
request is needed. The Metrics view shows usage as `used / quota`. A
refresh requested while the poller is still collecting per-row stats starts
the next cycle at once instead of waiting behind them. `space` toggles a `✓` mark on the selected pipeline in
place, k9s-style. `shift-↑/↓` marks a range the way a spreadsheet does: it
grows from the row where shift was first pressed and reversing direction
shrinks it. Marks stay put when you move without shift, so a range and a
few scattered `space` marks combine into one selection; the next `shift`
press starts a fresh range at the cursor. `esc` clears every mark and
`enter` collapses to the selected one. Lifecycle actions and commands then
apply to every marked pipeline, behind a single confirmation when one is
needed. `:delete` removes a pipeline permanently; it always
confirms, and queued behind `x` and `C` it tears a pipeline down completely.
Double-clicking a row jumps by column: name and tags open the SQL, the
metric columns open Metrics, status and errors open Logs.

A lifecycle request shows on its row as a `↻start`-style badge the moment
it is sent, until the next overview confirms the new status.

A lifecycle action requested while another request for the same pipeline is
still in flight queues behind it and shows as a `⧗` badge on the row, as do
start and clear while the pipeline is between states, because the server
refuses those until it settles. Stop, force-stop, and restart are accepted
in every state, so they go out at once, or at the next refresh when they
were queued behind an in-flight request; pause and resume go out as soon as
the pipeline answers `/stats`. Queued steps wait through intermediate states
(a checkpointing stop passes through `Suspended` before `Stopped`), fire
once the pipeline settles into the state they need, are skipped when their
goal already holds (a queued stop on an already-stopped pipeline), and
expire after ten minutes so a forgotten destructive step cannot fire much
later. So `x` then `C` stops the pipeline and clears it as soon as it
reaches `Stopped`.

Runtime metrics blank out once a pipeline stops answering `/stats` instead
of freezing at their last values; the storage column keeps showing the
last-known footprint (dimmed) while storage is in use, empties once it is
cleared, and burns the size to dust while a clear is running.

## Internal instances

The tailnet instances accept short-lived tsidp tokens (see the
`feldera-instance-access` skill). Tokens expire after five minutes, so point
f5a at the mint script instead of a static key; it re-runs the command as
the token ages:

```bash
f5a --host https://amd64-eks.staging.feldera.io --auth-command feldera-tsidp-token
```

Commands: `:start|pause|resume|stop|force-stop|restart|clear [pipeline]`
(`:restart` force-stops, waits for `Stopped`, and starts again, like
`fda restart`), `:samply N`, `:bundle`, `:profile`, `:sql`, `:sort rps`,
`:filter text`, `:tenant name`, `:refresh-every N`, `:quit`.

## Samply CPU profiles

`:samply 30` records the selected pipeline (or every marked one) with the
server's built-in [samply](https://github.com/mstange/samply) profiler for
30 seconds and saves the profile when the recording is done. A `SAMPLY`
column appears at the right edge while any capture exists and disappears
again once the last one fades; the status line carries the detail:

| Phase | SAMPLY column | Status line |
|-------|---------------|-------------|
| Recording | `▂▄▆█ 12s/30s`, a scrolling waveform | progress bar and countdown |
| Fetching | `⇣ ▱▰▱▱`, a travelling block | the server is still symbolicating |
| Saved | `✓ saved` | the path of the profile |
| Failed | `✖ failed` | the server's error |

The profile lands in the download directory as
`<pipeline>-samply-<millis>.json.gz`; open it with `samply load <file>`.
`--download-dir` (or `FELDERA_DOWNLOAD_DIR`) picks the directory; without
it f5a follows the OS convention for downloads: `XDG_DOWNLOAD_DIR` from the
XDG user dirs on Linux, `~/Downloads` on macOS, the `Downloads` known folder
on Windows, and the working directory when none of those exists. Finished
captures stay visible for five minutes.

`b` opens the selected pipeline's saved profile in the browser through
`samply load`, as does a double-click on its `SAMPLY` cell or a click on the
saved path in the status line. The viewer serves the profile from a local
port (3000 upward); f5a starts one per profile, points repeat opens at the
running one, and stops all of them when it quits. A tab left open keeps
what it loaded but cannot reload after that; `samply load <file>` from the
download directory brings the profile back.

## Support bundles

`:bundle` downloads the selected pipeline's support bundle (or one per
marked pipeline): `GET /v0/pipelines/{p}/support_bundle` collects logs,
configuration, profiles, metrics, and events into one zip, for any
deployment state. A `BUNDLE` column appears at the right edge with the same
travelling block while the server collects and the download runs, the same
blink once `<pipeline>-support-bundle-<millis>.zip` is in the download
directory, and the same retries if the transfer breaks. `B`, a double-click
on the `BUNDLE` cell, or a click on the saved path in the status line shows
the zip in the file manager: Finder selects the file on macOS (`open -R`),
elsewhere `xdg-open` opens its folder.

`:bundle settings` opens a dialog that shapes the request for the rest of
the session: a checkbox for collecting fresh data first, a stepper for how
many collections to include (current only, last 2, 3, 5, 10, or all the
server retains), one checkbox per section (circuit profile, heap profile,
metrics, logs, stats, pipeline configuration, system configuration,
dataflow graph, pipeline events), and a `Download now` row. `space` or
`enter` toggles, `←`/`→` (or `h`/`l`) step the count, `enter` on `Download
now` starts the download, `esc` closes. The default gathers everything now
and returns only that fresh collection; it maps onto the endpoint's
`collect`, `limit`, and per-section query parameters, and only settings
that differ from the server's defaults are sent.

The Firefox Profiler web app does not run in Safari, so f5a picks the
browser instead of relying on the system default: `$BROWSER` when set (a
command line, the URL is appended), else Firefox, else a Chromium-based
browser (Chrome, Chromium, Brave, Edge), else the platform opener (`open`
or `xdg-open`). On macOS the check looks for the application bundle in
`/Applications` or `~/Applications`; elsewhere for the executable on `PATH`.

Under the hood `POST /v0/pipelines/{p}/samply_profile?duration_secs=N`
answers 202 as soon as sampling starts, and
`GET /v0/pipelines/{p}/samply_profile?latest=true` answers 204 with a
`Retry-After` header until the profile exists; f5a polls at that cadence and
gives up after five minutes. The download runs under its own 15-minute
limit rather than `--timeout-secs`, because a profile of a busy pipeline
reaches tens of megabytes, and a download that breaks mid-transfer is
retried three times against the profile the server keeps.

## How the profiler works

1. `GET /v0/pipelines/{p}/circuit_json_profile` returns per-worker operator
   metrics keyed by circuit node id; readings are summed across workers.
2. `GET /v0/pipelines/{p}/dataflow_graph` returns the SQL compiler's MIR,
   where each node carries a `persistent_id` and SQL source positions.
3. Both sides share the `persistent_id` (circuit ids may add a role suffix
   such as `.shard_accintegral`, which is stripped for the join), so runtime
   cost maps to relations and lines of SQL.
4. The SQL view converts the joined costs into a per-line heat gutter.

## Architecture

| Layer | Modules | Notes |
|-------|---------|-------|
| Domain | `model::*` | Tolerant `serde_json::Value` -> typed views; a missing field is "unknown", never an error |
| Transport | `http`, `gateway` | Reads are raw JSON (survives server version skew); writes use the generated `feldera-rest-api` client |
| State | `app::*` | Pure `Msg -> state -> Vec<Cmd>` reducer, no IO |
| Runtime | `poll`, `runner` | Background poller streams updates over a channel; the UI never blocks on the network |
| Presentation | `ui::*` | Pure rendering from `&App` |

Reads avoid the generated OpenAPI types deliberately: strict deserialization
breaks whenever console and server versions drift (the source of the old
"unexpected response body" failure). Lifecycle writes return empty bodies, so
the typed client is safe and self-documenting there.

## Tests

```bash
cargo test -p f5a
cargo llvm-cov -p f5a --summary-only
```

The suite covers the domain mappers (fixtures mirror live payloads), the
reducer (every key path), the poller and dispatcher (scripted fake gateway),
the HTTP layer (wiremock), and every view (rendered into a `TestBackend`).
The event loop runs headless in tests via an injected event stream. Only
`main` and the raw-terminal guard are untested.
