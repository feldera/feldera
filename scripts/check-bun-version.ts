// Exits with an error if the running Bun is older than MIN_BUN_VERSION.
//
// The `deps` scripts build the workspace libraries with `bun run --filter`. Bun runs the
// packages in dependency order only from 1.3.10 on. Older versions build all of them at once,
// so a library can build before the libraries that it imports.

const MIN_BUN_VERSION = '1.3.10'

if (Bun.semver.order(Bun.version, MIN_BUN_VERSION) < 0) {
  console.error(
    `Bun ${Bun.version} is too old: the workspace scripts need Bun ${MIN_BUN_VERSION} or newer.\n` +
      'Run: bun upgrade'
  )
  process.exit(1)
}
