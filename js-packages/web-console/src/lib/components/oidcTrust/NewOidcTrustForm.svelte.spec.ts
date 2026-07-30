/**
 * The issuer field must be an http(s) URL. `va.url` alone was insufficient: the
 * URL constructor accepts any scheme, so `htt:/localhost:5173` parsed as valid.
 * These tests drive the exported schema directly (no render needed).
 */

import * as va from 'valibot'
import { describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-svelte'

// The component's instance script imports this at module load; stub it so the
// schema import does not drag in the real service.
vi.mock('$lib/services/pipelineManager', () => ({ postOidcTrust: vi.fn() }))

// Imported AFTER vi.mock so the mock takes effect.
import NewOidcTrustForm, { oidcTrustSchema } from './NewOidcTrustForm.svelte'

const base = {
  name: 'ci',
  issuer: 'https://issuer.example',
  subject: 'repo:org/repo',
  audience: '',
  description: '',
  role: 'read' as const
}

// abortPipeEarly mirrors the adapter config in the form, so the reported issue
// is the single first-failing check the user actually sees.
const issuerErrors = (issuer: string): string[] => {
  const result = va.safeParse(oidcTrustSchema, { ...base, issuer }, { abortPipeEarly: true })
  return result.success
    ? []
    : result.issues.filter((i) => i.path?.[0]?.key === 'issuer').map((i) => i.message)
}

describe('oidcTrustSchema — issuer validation', () => {
  it('accepts http(s) issuer URLs', () => {
    expect(issuerErrors('https://token.actions.githubusercontent.com')).toEqual([])
    expect(issuerErrors('http://localhost:5173')).toEqual([])
  })

  it('rejects a URL whose scheme is not http(s)', () => {
    // Regression: new URL('htt:/localhost:5173') parses fine, so va.url passed it.
    expect(issuerErrors('htt:/localhost:5173')).toEqual(['The issuer must be an http(s) URL'])
    expect(issuerErrors('ftp://example.com')).toEqual(['The issuer must be an http(s) URL'])
  })

  it('rejects a malformed URL even with an http(s) scheme', () => {
    // The scheme regex alone accepts this ('^' matches .+); va.url rejects it,
    // since '^' is a forbidden host code point, so new URL throws.
    expect(issuerErrors('http://a^b')).toEqual(['The issuer must be a valid URL'])
  })

  it('rejects a string that is not a URL', () => {
    expect(issuerErrors('not a url')).toEqual(['The issuer must be a valid URL'])
  })

  it('requires the issuer to be present', () => {
    expect(issuerErrors('')).toEqual(['Specify the issuer URL'])
  })
})

describe('NewOidcTrustForm', () => {
  // superforms' valibot adapter converts the schema to JSON Schema on mount to
  // derive form constraints; an unconvertible action (a `va.check` predicate, or
  // a flagged regex) throws there and the form never renders. This asserts the
  // schema stays convertible.
  it('mounts without the adapter failing to convert the schema', async () => {
    const target = document.createElement('div')
    document.body.appendChild(target)
    const mounted = render(NewOidcTrustForm, { target }) as any
    try {
      await expect
        .poll(() => document.querySelector('input[placeholder="github-actions-prod"]'))
        .toBeTruthy()
    } finally {
      await mounted.unmount()
      target.remove()
    }
  })
})
