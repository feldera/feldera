import { describe, expect, it } from 'vitest'
import { readPastedLinkFiles, resolvePastedLinkPipelineName } from './usePastedLink.svelte'

const read = (query: string) => readPastedLinkFiles(new URLSearchParams(query))

describe('pasted /create/ link', () => {
  it('reads the code from a docs "Run" link', () => {
    expect(read('name=docs-abc&code=SELECT%201')).toEqual({
      'program.sql': 'SELECT 1',
      'udf.rs': '',
      'udf.toml': ''
    })
  })

  it('prefers program.sql over the legacy code parameter', () => {
    expect(read('name=p&code=legacy&program.sql=current')?.['program.sql']).toBe('current')
  })

  it('reads a link with only Rust code as carrying code', () => {
    expect(read('name=p&udf.rs=fn%20f()%20%7B%7D')?.['udf.rs']).toBe('fn f() {}')
    expect(read('name=p&udf.toml=x')?.['udf.toml']).toBe('x')
  })

  it('reads a demo link or empty code parameters as carrying no code', () => {
    expect(read('name=demo')).toBeUndefined()
    expect(read('name=demo&program.sql=&code=&udf.rs=&udf.toml=')).toBeUndefined()
  })

  it('picks a free name when the link names an existing pipeline', () => {
    expect(resolvePastedLinkPipelineName('p', [{ name: 'q' }])).toBe('p')
    expect(resolvePastedLinkPipelineName('p', [{ name: 'p' }, { name: 'p-copy' }])).toBe('p-copy2')
  })
})
