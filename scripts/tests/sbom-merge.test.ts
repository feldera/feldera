import { afterEach, beforeEach, describe, expect, test } from 'bun:test'
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'

const scriptPath = path.resolve(import.meta.dir, '../sbom-merge.mts')
const imageRootId = 'SPDXRef-DocumentRoot-Image-pipeline-manager'

type Component = {
  name: string
  group?: string
  version?: string
  purl?: string
  licenses?: object[]
  components?: Component[]
}

const npm = (name: string, version?: string, extra: Partial<Component> = {}): Component => ({
  name,
  version,
  purl: `pkg:npm/${name}${version ? `@${version}` : ''}`,
  ...extra
})

const appSbom = (app: string, components: Component[]) => ({
  metadata: { component: { name: app, purl: `pkg:generic/feldera/${app}@1.0.0` } },
  components
})

// A minimal image SBOM in the shape that syft writes
const imageSbom = () => ({
  spdxVersion: 'SPDX-2.3',
  packages: [
    {
      name: 'pipeline-manager',
      SPDXID: imageRootId,
      externalRefs: [
        {
          referenceCategory: 'PACKAGE-MANAGER',
          referenceType: 'purl',
          referenceLocator: 'pkg:oci/pipeline-manager'
        }
      ]
    },
    {
      name: 'libc6',
      SPDXID: 'SPDXRef-Package-deb-libc6',
      externalRefs: [
        {
          referenceCategory: 'PACKAGE-MANAGER',
          referenceType: 'purl',
          referenceLocator: 'pkg:deb/ubuntu/libc6@2.39'
        }
      ]
    }
  ],
  relationships: [
    {
      spdxElementId: 'SPDXRef-DOCUMENT',
      relatedSpdxElement: imageRootId,
      relationshipType: 'DESCRIBES'
    },
    {
      spdxElementId: imageRootId,
      relatedSpdxElement: 'SPDXRef-Package-deb-libc6',
      relationshipType: 'CONTAINS'
    }
  ]
})

type SpdxPackage = {
  name: string
  SPDXID: string
  versionInfo?: string
  sourceInfo?: string
  licenseDeclared?: string
  externalRefs?: { referenceType: string; referenceLocator: string }[]
}
type SpdxResult = {
  packages: SpdxPackage[]
  relationships: { spdxElementId: string; relatedSpdxElement: string; relationshipType: string }[]
}

let workDir: string
beforeEach(async () => {
  workDir = await mkdtemp(path.join(tmpdir(), 'sbom-merge-'))
})
afterEach(async () => {
  await rm(workDir, { recursive: true, force: true })
})

const writeJson = async (name: string, value: object) => {
  const filePath = path.join(workDir, name)
  await writeFile(filePath, JSON.stringify(value))
  return filePath
}

const run = (runtime: string, args: string[]) => {
  const result = Bun.spawnSync([runtime, scriptPath, ...args], { stderr: 'pipe', stdout: 'pipe' })
  return { exitCode: result.exitCode, stderr: result.stderr.toString() }
}

// The feldera workflow runs the script with Node.js, the cloud workflow with Bun
describe.each([['node'], [process.execPath]])('sbom-merge with %s', (runtime) => {
  // Merges the app SBOMs into `image` and returns the npm packages that the script added
  const merge = async (apps: object[], image: object = imageSbom()) => {
    const imagePath = await writeJson('image.spdx.json', image)
    const appPaths = await Promise.all(apps.map((app, i) => writeJson(`app${i}.cdx.json`, app)))
    const { exitCode, stderr } = run(runtime, [imagePath, ...appPaths])
    expect(stderr).toBe('')
    expect(exitCode).toBe(0)
    const result = JSON.parse(await readFile(imagePath, 'utf8')) as SpdxResult
    const added = result.packages.filter((pkg) => pkg.SPDXID.startsWith('SPDXRef-Package-npm-'))
    return { result, added }
  }

  const purlsOf = (packages: SpdxPackage[]) =>
    packages.map(
      (pkg) => pkg.externalRefs?.find((ref) => ref.referenceType === 'purl')?.referenceLocator
    )

  test('adds an npm package that the image root contains', async () => {
    const { result, added } = await merge([
      appSbom('web-console', [
        npm('oidc-client', '7.26.4', {
          group: '@axa-fr',
          purl: 'pkg:npm/%40axa-fr/oidc-client@7.26.4?vcs_url=git%2Bhttps%3A%2F%2Fgithub.com%2FAxaFrance%2Foidc-client.git',
          licenses: [{ license: { id: 'MIT' } }]
        })
      ])
    ])
    expect(added).toEqual([
      expect.objectContaining({
        name: '@axa-fr/oidc-client',
        versionInfo: '7.26.4',
        licenseDeclared: 'MIT',
        sourceInfo: 'acquired package info from the CycloneDX SBOM of the bundle: web-console',
        externalRefs: [
          {
            referenceCategory: 'PACKAGE-MANAGER',
            referenceType: 'purl',
            referenceLocator: 'pkg:npm/%40axa-fr/oidc-client@7.26.4'
          }
        ]
      })
    ])
    expect(result.relationships).toContainEqual({
      spdxElementId: imageRootId,
      relatedSpdxElement: added[0].SPDXID,
      relationshipType: 'CONTAINS'
    })
  })

  test('keeps the packages and relationships of the image', async () => {
    const image = imageSbom()
    const { result } = await merge([appSbom('web-console', [npm('echarts', '6.1.0')])], image)
    expect(result.packages.slice(0, 2)).toEqual(image.packages)
    expect(result.relationships.slice(0, 2)).toEqual(image.relationships)
  })

  test('adds a package that both apps contain once, and names both apps', async () => {
    const { added } = await merge([
      appSbom('web-console', [npm('dompurify', '3.2.7')]),
      appSbom('profiler-app', [npm('dompurify', '3.2.7')])
    ])
    expect(added).toHaveLength(1)
    expect(added[0].sourceInfo).toBe(
      'acquired package info from the CycloneDX SBOM of the bundle: web-console, profiler-app'
    )
  })

  test('names an app once when it contains a package twice', async () => {
    const { added } = await merge([
      appSbom('web-console', [
        npm('dompurify', '3.2.7'),
        npm('monaco-editor', '0.55.1', { components: [npm('dompurify', '3.2.7')] })
      ])
    ])
    expect(added.find((pkg) => pkg.name === 'dompurify')?.sourceInfo).toBe(
      'acquired package info from the CycloneDX SBOM of the bundle: web-console'
    )
  })

  test('treats purls that differ only in qualifiers as one package', async () => {
    const { added } = await merge([
      appSbom('web-console', [npm('fflate', '0.4.9', { purl: 'pkg:npm/fflate@0.4.9?vcs_url=a' })]),
      appSbom('profiler-app', [npm('fflate', '0.4.9', { purl: 'pkg:npm/fflate@0.4.9#sub/path' })])
    ])
    expect(purlsOf(added)).toEqual(['pkg:npm/fflate@0.4.9'])
  })

  test('adds the vendored packages as flat packages of the image', async () => {
    const { result, added } = await merge([
      appSbom('web-console', [
        npm('posthog-js', '1.300.0', {
          components: [npm('@posthog/core', '1.23.1', { components: [npm('fflate', '0.4.9')] })]
        })
      ])
    ])
    expect(added.map((pkg) => pkg.name).sort()).toEqual(['@posthog/core', 'fflate', 'posthog-js'])
    const containedIds = result.relationships
      .filter((rel) => rel.spdxElementId === imageRootId && rel.relationshipType === 'CONTAINS')
      .map((rel) => rel.relatedSpdxElement)
    expect(containedIds).toEqual(expect.arrayContaining(added.map((pkg) => pkg.SPDXID)))
  })

  test('adds each version of a package as a separate package', async () => {
    const { added } = await merge([
      appSbom('web-console', [
        npm('dompurify', '3.4.17'),
        npm('monaco-editor', '0.55.1', { components: [npm('dompurify', '3.2.7')] })
      ])
    ])
    const dompurify = added.filter((pkg) => pkg.name === 'dompurify')
    expect(dompurify.map((pkg) => pkg.versionInfo).sort()).toEqual(['3.2.7', '3.4.17'])
    expect(dompurify[0].SPDXID).not.toBe(dompurify[1].SPDXID)
  })

  test('skips components that are not npm packages', async () => {
    const { added } = await merge([
      appSbom('web-console', [
        { name: 'no-purl', version: '1.0.0' },
        { name: 'generic', version: '1.0.0', purl: 'pkg:generic/feldera/generic@1.0.0' },
        npm('echarts', '6.1.0')
      ])
    ])
    expect(added.map((pkg) => pkg.name)).toEqual(['echarts'])
  })

  test('skips a package that the image already lists', async () => {
    const image = imageSbom()
    image.packages.push({
      name: 'echarts',
      SPDXID: 'SPDXRef-Package-syft-echarts',
      externalRefs: [
        {
          referenceCategory: 'PACKAGE-MANAGER',
          referenceType: 'purl',
          referenceLocator: 'pkg:npm/echarts@6.1.0?x=1'
        }
      ]
    })
    const { result } = await merge([appSbom('web-console', [npm('echarts', '6.1.0')])], image)
    expect(result.packages).toHaveLength(3)
    expect(result.relationships).toHaveLength(2)
  })

  test('adds nothing when the script runs a second time', async () => {
    const apps = [appSbom('web-console', [npm('echarts', '6.1.0')])]
    const { result: first } = await merge(apps)
    const { result: second } = await merge(apps, first)
    expect(second).toEqual(first)
  })

  test('accepts an image package without external references', async () => {
    const image = imageSbom()
    image.packages.push({ name: 'file-only', SPDXID: 'SPDXRef-Package-file-only' } as never)
    const { added } = await merge([appSbom('web-console', [npm('echarts', '6.1.0')])], image)
    expect(added.map((pkg) => pkg.name)).toEqual(['echarts'])
  })

  test('accepts an app SBOM without components', async () => {
    const { result } = await merge([{ metadata: { component: { name: 'profiler-app' } } }])
    expect(result).toEqual(imageSbom())
  })

  test.each([
    ['one license id', [{ license: { id: 'MIT' } }], 'MIT'],
    [
      'a license expression',
      [{ expression: '(MPL-2.0 OR Apache-2.0)' }],
      '(MPL-2.0 OR Apache-2.0)'
    ],
    [
      'two license ids',
      [{ license: { id: 'MIT' } }, { license: { id: 'ISC' } }],
      '(MIT) AND (ISC)'
    ],
    ['a license name without id', [{ license: { name: 'SEE LICENSE IN LICENSE' } }], 'NOASSERTION'],
    [
      'an id and a name',
      [{ license: { id: 'MIT' } }, { license: { name: 'Custom' } }],
      'NOASSERTION'
    ],
    ['no licenses', undefined, 'NOASSERTION'],
    ['an empty license list', [], 'NOASSERTION']
  ])('declares the license of a package with %s', async (_, licenses, expected) => {
    const { added } = await merge([appSbom('web-console', [npm('pkg', '1.0.0', { licenses })])])
    expect(added[0].licenseDeclared).toBe(expected)
  })

  test('gives a package without a version the version NOASSERTION', async () => {
    const { added } = await merge([appSbom('web-console', [npm('unversioned')])])
    expect(added[0].versionInfo).toBe('NOASSERTION')
  })

  test('gives each package a valid and unique SPDX ID', async () => {
    const { added } = await merge([
      appSbom('web-console', [
        npm('a.b', '1.0.0'),
        npm('b', '1.0.0', { group: '@a', purl: 'pkg:npm/%40a/b@1.0.0' }),
        npm('a-b', '1.0.0'),
        npm('a_b', '1.0.0')
      ])
    ])
    const ids = added.map((pkg) => pkg.SPDXID)
    expect(new Set(ids).size).toBe(4)
    for (const id of ids) {
      // SPDX 2.3 allows only letters, digits, `.` and `-` after `SPDXRef-`
      expect(id).toMatch(/^SPDXRef-[A-Za-z0-9.-]+$/)
    }
  })

  test.each([
    ['no arguments', []],
    ['no app SBOM', ['image.spdx.json']]
  ])('prints the usage and fails with %s', (_, args) => {
    const { exitCode, stderr } = run(runtime, args)
    expect(exitCode).toBe(1)
    expect(stderr).toContain('Usage: node scripts/sbom-merge.mts')
  })

  test('fails and leaves the image SBOM unchanged when it does not describe an image', async () => {
    const image = imageSbom()
    image.relationships.shift()
    const imagePath = await writeJson('image.spdx.json', image)
    const appPath = await writeJson(
      'app.cdx.json',
      appSbom('web-console', [npm('echarts', '6.1.0')])
    )
    const { exitCode, stderr } = run(runtime, [imagePath, appPath])
    expect(exitCode).not.toBe(0)
    expect(stderr).toContain('has no DESCRIBES relationship')
    expect(JSON.parse(await readFile(imagePath, 'utf8'))).toEqual(image)
  })

  test('fails and leaves the image SBOM unchanged when an app SBOM is missing', async () => {
    const image = imageSbom()
    const imagePath = await writeJson('image.spdx.json', image)
    const { exitCode } = run(runtime, [imagePath, path.join(workDir, 'missing.cdx.json')])
    expect(exitCode).not.toBe(0)
    expect(JSON.parse(await readFile(imagePath, 'utf8'))).toEqual(image)
  })
})
