// Adds the npm packages of the web-console and profiler-app SBOMs (see `scripts/sbom.ts`) to an
// SPDX image SBOM that syft wrote, because syft cannot see the npm packages compiled into the
// pipeline-manager binary.
//
// Usage: node scripts/sbom-merge.mts <image.spdx.json> <app.cdx.json>...
// Rewrites <image.spdx.json> in place. Each package occurs once per name and version, without the
// nesting of vendored packages in the CycloneDX SBOMs.

import { createHash } from 'node:crypto'
import { readFile, writeFile } from 'node:fs/promises'

type CycloneDxLicense = { license?: { id?: string }; expression?: string }

type CycloneDxComponent = {
  purl?: string
  name: string
  group?: string
  version?: string
  licenses?: CycloneDxLicense[]
  components?: CycloneDxComponent[]
}

type SpdxPackage = {
  name: string
  SPDXID: string
  versionInfo: string
  supplier: string
  downloadLocation: string
  filesAnalyzed: boolean
  sourceInfo: string
  licenseConcluded: string
  licenseDeclared: string
  copyrightText: string
  // Optional in SPDX 2.3
  externalRefs?: {
    referenceCategory: string
    referenceType: string
    referenceLocator: string
  }[]
}

type SpdxDocument = {
  packages: SpdxPackage[]
  relationships: {
    spdxElementId: string
    relatedSpdxElement: string
    relationshipType: string
  }[]
}

const [imageSbomPath, ...appSbomPaths] = process.argv.slice(2)
if (!imageSbomPath || appSbomPaths.length === 0) {
  console.error('Usage: node scripts/sbom-merge.mts <image.spdx.json> <app.cdx.json>...')
  process.exit(1)
}

// Calls `visit` for every component, and for the components that it contains
const forEachComponent = (
  components: CycloneDxComponent[] | undefined,
  visit: (component: CycloneDxComponent) => void
) => {
  for (const component of components ?? []) {
    visit(component)
    forEachComponent(component.components, visit)
  }
}

// The qualifiers (e.g. `?vcs_url=...`) do not identify the package, so two apps can list the same
// package with different qualifiers
const stripPurlQualifiers = (purl: string) => purl.replace(/[?#].*$/, '')

// SPDX allows only one license expression per package, so the script joins the licenses with AND.
// A license without an SPDX ID or expression makes the result NOASSERTION.
const toSpdxLicense = (licenses: CycloneDxLicense[] | undefined) => {
  const expressions = (licenses ?? []).map(
    (license) => license.expression ?? license.license?.id ?? 'NOASSERTION'
  )
  if (expressions.length === 0 || expressions.includes('NOASSERTION')) {
    return 'NOASSERTION'
  }
  return expressions.length === 1 ? expressions[0] : expressions.map((e) => `(${e})`).join(' AND ')
}

// Uses the ID format of syft. The name keeps only the characters that SPDX IDs allow, and the
// hash of the purl makes the ID unique.
const toSpdxId = (name: string, purl: string) => {
  const idName = name.replace(/[^A-Za-z0-9.-]/g, '-')
  const purlHash = createHash('sha256').update(purl).digest('hex').slice(0, 16)
  return `SPDXRef-Package-npm-${idName}-${purlHash}`
}

const imageSbom = JSON.parse(await readFile(imageSbomPath, 'utf8')) as SpdxDocument
const imageRootId = imageSbom.relationships.find(
  (relationship) =>
    relationship.spdxElementId === 'SPDXRef-DOCUMENT' &&
    relationship.relationshipType === 'DESCRIBES'
)?.relatedSpdxElement
if (!imageRootId) {
  throw new Error(`${imageSbomPath} has no DESCRIBES relationship for the image`)
}

// Packages by purl without qualifiers, with the names of the apps that contain them
const npmPackages = new Map<string, { component: CycloneDxComponent; apps: string[] }>()
for (const appSbomPath of appSbomPaths) {
  const appSbom = JSON.parse(await readFile(appSbomPath, 'utf8')) as {
    metadata: { component: { name: string } }
    components?: CycloneDxComponent[]
  }
  const app = appSbom.metadata.component.name
  forEachComponent(appSbom.components, (component) => {
    if (!component.purl?.startsWith('pkg:npm/')) {
      return
    }
    const purl = stripPurlQualifiers(component.purl)
    const entry = npmPackages.get(purl) ?? { component, apps: [] }
    entry.apps.push(app)
    npmPackages.set(purl, entry)
  })
}

const imagePurls = new Set(
  imageSbom.packages.flatMap((pkg) =>
    (pkg.externalRefs ?? [])
      .filter((ref) => ref.referenceType === 'purl')
      .map((ref) => stripPurlQualifiers(ref.referenceLocator))
  )
)

// syft can find some npm packages in the image itself, e.g. from a package.json file
let addedCount = 0
for (const [purl, { component, apps }] of npmPackages) {
  if (imagePurls.has(purl)) {
    continue
  }
  const name = component.group ? `${component.group}/${component.name}` : component.name
  const spdxId = toSpdxId(name, purl)
  const bundles = [...new Set(apps)].join(', ')
  imageSbom.packages.push({
    name,
    SPDXID: spdxId,
    versionInfo: component.version ?? 'NOASSERTION',
    supplier: 'NOASSERTION',
    downloadLocation: 'NOASSERTION',
    filesAnalyzed: false,
    sourceInfo: `acquired package info from the CycloneDX SBOM of the bundle: ${bundles}`,
    licenseConcluded: 'NOASSERTION',
    licenseDeclared: toSpdxLicense(component.licenses),
    copyrightText: 'NOASSERTION',
    externalRefs: [
      {
        referenceCategory: 'PACKAGE-MANAGER',
        referenceType: 'purl',
        referenceLocator: purl
      }
    ]
  })
  imageSbom.relationships.push({
    spdxElementId: imageRootId,
    relatedSpdxElement: spdxId,
    relationshipType: 'CONTAINS'
  })
  addedCount++
}

await writeFile(imageSbomPath, JSON.stringify(imageSbom))
console.log(`Added ${addedCount} npm packages to ${imageSbomPath}`)
