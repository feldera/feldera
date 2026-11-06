import { describe, expect, test } from 'bun:test'
import { type CycloneDxBom, selectReleaseVersion, setRootVersion } from '../sbom'

const cargoToml = (version: string) => `[workspace.package]\nversion = "${version}"\n`

describe('selectReleaseVersion', () => {
  test('uses JS_SBOM_VERSION when it is set', () => {
    expect(selectReleaseVersion('9.8.7', cargoToml('0.1.0'))).toBe('9.8.7')
  })

  test('uses the workspace version of Cargo.toml when JS_SBOM_VERSION is not set', () => {
    expect(selectReleaseVersion(undefined, cargoToml('0.1.0'))).toBe('0.1.0')
  })

  test('uses the workspace version of Cargo.toml when JS_SBOM_VERSION is empty', () => {
    expect(selectReleaseVersion('', cargoToml('0.1.0'))).toBe('0.1.0')
  })

  test('fails when Cargo.toml has no workspace version', () => {
    expect(() => selectReleaseVersion(undefined, '[package]\nversion = "0.1.0"\n')).toThrow()
  })
})

const placeholderRef = 'pkg:npm/web-console@0.0.1'

const makeBom = (dependencies?: CycloneDxBom['dependencies']): CycloneDxBom => ({
  metadata: {
    component: {
      name: 'web-console',
      version: '0.0.1',
      purl: placeholderRef,
      'bom-ref': placeholderRef
    }
  },
  dependencies
})

describe('setRootVersion', () => {
  test('gives the root component the version and a generic purl', () => {
    const bom = makeBom()
    setRootVersion(bom, '9.8.7')
    expect(bom.metadata.component).toEqual({
      name: 'web-console',
      version: '9.8.7',
      purl: 'pkg:generic/feldera/web-console@9.8.7',
      'bom-ref': 'pkg:generic/feldera/web-console@9.8.7'
    })
  })

  test('renames the root in the dependency graph and keeps the other refs', () => {
    const bom = makeBom([
      { ref: placeholderRef, dependsOn: ['pkg:npm/echarts@6.1.0'] },
      { ref: 'pkg:npm/echarts@6.1.0', dependsOn: [placeholderRef, 'pkg:npm/zrender@6.0.0'] },
      { ref: 'pkg:npm/zrender@6.0.0' }
    ])
    setRootVersion(bom, '9.8.7')
    expect(JSON.parse(JSON.stringify(bom.dependencies))).toEqual([
      { ref: 'pkg:generic/feldera/web-console@9.8.7', dependsOn: ['pkg:npm/echarts@6.1.0'] },
      {
        ref: 'pkg:npm/echarts@6.1.0',
        dependsOn: ['pkg:generic/feldera/web-console@9.8.7', 'pkg:npm/zrender@6.0.0']
      },
      { ref: 'pkg:npm/zrender@6.0.0' }
    ])
  })

  test('accepts an SBOM without a dependency graph', () => {
    const bom = makeBom()
    setRootVersion(bom, '9.8.7')
    expect(bom.dependencies).toBeUndefined()
  })
})
