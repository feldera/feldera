// Fails when the running Bun is older than the version the workspace scripts need.
//
// The `deps` scripts build the workspace libraries with `bun run --filter`, which runs the
// packages in dependency order only from Bun 1.3.10 on. Older versions build them all at once,
// so a library can build before the libraries it imports.

const MIN_BUN_VERSION = '1.3.10'

if (Bun.semver.order(Bun.version, MIN_BUN_VERSION) < 0) {
  console.error(
    `Bun ${Bun.version} is too old: the workspace scripts need Bun ${MIN_BUN_VERSION} or newer.\n` +
      'Run: bun upgrade'
  )
  process.exit(1)
}
