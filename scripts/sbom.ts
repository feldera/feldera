// Writes CycloneDX SBOMs of the npm packages in the web-console and profiler-app bundles to
// build/sbom/<app>.cdx.json.
//
// The root component of each SBOM gets the release version. This version is JS_SBOM_VERSION if
// set, else the workspace version in Cargo.toml.

import { mkdir } from 'node:fs/promises'
import path from 'node:path'
import { $ } from 'bun'

const repoDir = path.resolve(import.meta.dir, '..')
const outDir = path.join(repoDir, 'build/sbom')

// The `build:sbom` script of each app writes its SBOM here
const appSboms = {
  'web-console': 'js-packages/web-console/build/sbom/cyclonedx/bom.json',
  'profiler-app': 'js-packages/profiler-app/dist/sbom/cyclonedx/bom.json'
}

export type CycloneDxBom = {
  metadata: {
    component: {
      name: string
      version: string
      purl: string
      'bom-ref': string
    }
  }
  dependencies?: { ref: string; dependsOn?: string[] }[]
}

export const selectReleaseVersion = (jsSbomVersion: string | undefined, cargoToml: string) => {
  if (jsSbomVersion) {
    return jsSbomVersion
  }
  const { workspace } = Bun.TOML.parse(cargoToml) as { workspace: { package: { version: string } } }
  return workspace.package.version
}

// The plugin takes the root component from the private package.json of the app, with a
// placeholder version. The new purl is generic because an unrelated public npm package can have
// the same name, e.g. `web-console`.
export const setRootVersion = (bom: CycloneDxBom, version: string) => {
  const root = bom.metadata.component
  const oldRef = root['bom-ref']
  const newRef = `pkg:generic/feldera/${root.name}@${version}`
  root.version = version
  root.purl = newRef
  root['bom-ref'] = newRef
  for (const dependency of bom.dependencies ?? []) {
    if (dependency.ref === oldRef) {
      dependency.ref = newRef
    }
    dependency.dependsOn = dependency.dependsOn?.map((ref) => (ref === oldRef ? newRef : ref))
  }
}

// Runs the build only when this file is the entry point, so that the tests can import the functions
if (import.meta.main) {
  const cargoToml = await Bun.file(path.join(repoDir, 'Cargo.toml')).text()
  const version = selectReleaseVersion(process.env.JS_SBOM_VERSION, cargoToml)

  $.cwd(repoDir)
  await $`bun run build:libs`
  await $`bun run --filter web-console --filter profiler-app build:sbom`

  await mkdir(outDir, { recursive: true })
  for (const [app, sbomPath] of Object.entries(appSboms)) {
    const bom = (await Bun.file(path.join(repoDir, sbomPath)).json()) as CycloneDxBom
    setRootVersion(bom, version)
    const outPath = path.join(outDir, `${app}.cdx.json`)
    await Bun.write(outPath, JSON.stringify(bom, null, '\t'))
    console.log(`Wrote ${outPath} (${app} ${version})`)
  }
}
