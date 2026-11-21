# Gen-2 compatibility measurement

The current local measurement passes **2,218/2,868 tests (77.34%)**, with 69
failures, 510 errors and 71 skips. Excluding skips gives **79.30%**. Both
denominators remain below the plan's 80% criterion. Gen-2 cannot yet serve as a
sufficient C7 oracle. The [certificate](evidence/gen2-ci-runner.json) retains
failure groups and references the complete raw reports; none is an approved
deviation.

`scripts/run_gen2_probe.py` first captures the current tree's compile/export
suite with runtime execution disabled. It then runs the full suite with
`GEN2_PATH` enabled and requires identical test identities. Both runs use fresh
report directories. Maven totals, XML totals and process status must agree.
The runner checks engine hashes before and after execution and refuses compiler
source changes during measurement. A missing report or lost test fails the
audit; a complete experiment below 80% records that failed criterion without
discarding failures or skips.

The source manifest includes each Maven module's POM and complete `src` tree,
including parser-generation configuration and non-Java test resources, plus
root build scripts and `.mvn` configuration. Generated build trees are excluded.
A regression test checks that resource changes invalidate the manifest while
generated Java and XML files do not. The repaired manifest matches all 1,213
inputs between the workspace and the writable devcontainer compiler copy.
The earlier measurement retains its original runner and manifest; this audit
does not retroactively expand its recorded inputs.

Run in the devcontainer, after building the Java compiler and engine:

```sh
python3 crates/sql/scripts/run_gen2_probe.py $SCRATCH/gen2-measurement \
  --compiler /path/to/sql-to-dbsp-compiler \
  --engine /path/to/engine-binaries \
  --engine-provenance /path/to/engine.json \
  --calcite-revision 4bf922bbbc1cbd570a2f201339a46a2d8f7a763b
```

The engine provenance JSON must contain complete `forge_commit` and
`forge_platform_commit` pins and a `binaries_sha256` map with `pipeline` and
`ir-expander` hashes. Use the engine's build record; a commit string alone does
not establish which executable ran. Shared-library paths must be configured for
the selected build. The local run reused the previously recorded ARM64 engine;
it was not an AMD64 CI run or a fresh engine build.

The on-demand `test-gen2.yml` workflow builds Forge revision
`d4879df7cded0fefb176aebd2a9890caa8877665` with platform revision
`43f386f2022e9c112cf9bc2aea326b0bd3178bcd`, then tests this repository's current
compiler. Start the workflow manually. A schedule requires a disclosure decision
from the plan owner. Artifacts expire after one day. It uses the existing `CI_ACCESS_APP_ID`/`CI_ACCESS_APP_PKEY` with
read-only access scoped to the internal Forge repository. That app installation
must permit this repository; remote checkout/build authorization has not been
validated in this session. The workflow retains measurement reports and hashes,
not internal engine binaries. No remote workflow was dispatched.

The v3 export registered 8,546 cases. It retained explicit skip reasons
for 17 cases from 16 JUnit methods. The audit joins each case to its original
compiler report and preserves the 2,868 JUnit identities behind the runtime
measurement. An earlier compiler snapshot stopped before registering two
NOW-equality cases because its lazy-variable declaration and initializer
had different types. The later let-rewrite fix recovers both cases and
passes canonical S2 replay. Decision 2 uses the classic C7 oracle and
canonical S3 equality for Gen-2. The recorded skips grant no gate exceptions.
[Corpus skip audit](evidence/gen2-corpus-skip-disposition.json) ·
[NOW-equality recovery](evidence/s2-let-rewrite-added.json)

The first local runner attempt failed because its baseline omitted corpus
provenance and used an ineffective report-directory override. Its logs remain
under `$SCRATCH/p0-gen2-ci-runner-v1`. The repaired run is
`$SCRATCH/p0-gen2-ci-runner-v2`; the Maven project now exposes
`feldera.test.reports`, preserving the default report location for ordinary runs.
The final baseline audit was also replayed against both completed report sets.
