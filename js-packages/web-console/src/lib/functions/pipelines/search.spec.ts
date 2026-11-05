import { describe, expect, it } from 'vitest'
import { findMatchingTags, matchesPipelineSearch, nameMatchesFirst } from './search'

const pipeline = (name: string, tags: string[] = []) => ({ name, tags })

const names = (pipelines: readonly { name: string }[]) => pipelines.map((p) => p.name)

describe('matchesPipelineSearch', () => {
  it('matches a substring of the name', () => {
    expect(matchesPipelineSearch(pipeline('fraud-detection'), 'detect', [])).toBe(true)
    expect(matchesPipelineSearch(pipeline('orders'), 'detect', [])).toBe(false)
  })

  it('matches a substring of a tag name', () => {
    expect(matchesPipelineSearch(pipeline('orders', ['production|ef4444']), 'duct', [])).toBe(true)
  })

  it('does not match the color part of a tag', () => {
    expect(matchesPipelineSearch(pipeline('orders', ['prod|ef4444']), 'ef44', [])).toBe(false)
  })

  it('trims the query', () => {
    expect(matchesPipelineSearch(pipeline('orders'), '  ord  ', [])).toBe(true)
    expect(matchesPipelineSearch(pipeline('orders', ['prod']), ' prod ', [])).toBe(true)
  })

  it('matches every pipeline when the query is empty or blank and no tag is picked', () => {
    expect(matchesPipelineSearch(pipeline('orders'), '', [])).toBe(true)
    expect(matchesPipelineSearch(pipeline('orders'), '   ', [])).toBe(true)
  })

  it('requires every picked tag', () => {
    const both = pipeline('a', ['prod|ef4444', 'team-a|22c55e'])
    const prodOnly = pipeline('b', ['prod|ef4444'])
    const filters = ['prod|ef4444', 'team-a|22c55e']
    expect(matchesPipelineSearch(both, '', filters)).toBe(true)
    expect(matchesPipelineSearch(prodOnly, '', filters)).toBe(false)
  })

  it('compares picked tags by name, not by color', () => {
    expect(matchesPipelineSearch(pipeline('a', ['prod|22c55e']), '', ['prod|ef4444'])).toBe(true)
    expect(matchesPipelineSearch(pipeline('a', ['prod']), '', ['prod|ef4444'])).toBe(true)
  })

  it('applies the query and the picked tags together', () => {
    const p = pipeline('orders', ['prod'])
    expect(matchesPipelineSearch(p, 'ord', ['prod'])).toBe(true)
    expect(matchesPipelineSearch(p, 'pay', ['prod'])).toBe(false)
    expect(matchesPipelineSearch(p, 'ord', ['team-a'])).toBe(false)
  })
})

describe('nameMatchesFirst', () => {
  const pipelines = [
    pipeline('alpha', ['prod']),
    pipeline('prod-ingest'),
    pipeline('beta', ['prod']),
    pipeline('prod-export')
  ]

  it('puts name matches first and keeps the order inside each group', () => {
    expect(names(nameMatchesFirst(pipelines, 'prod'))).toEqual([
      'prod-ingest',
      'prod-export',
      'alpha',
      'beta'
    ])
  })

  it('trims the query', () => {
    expect(names(nameMatchesFirst(pipelines, ' prod '))[0]).toBe('prod-ingest')
  })

  it('returns the input unchanged for an empty or blank query', () => {
    expect(nameMatchesFirst(pipelines, '')).toBe(pipelines)
    expect(nameMatchesFirst(pipelines, '  ')).toBe(pipelines)
  })
})

describe('findMatchingTags', () => {
  it('returns one tag per name when pipelines use different colors', () => {
    const pipelines = [pipeline('a', ['prod|ef4444']), pipeline('b', ['prod|22c55e'])]
    expect(findMatchingTags(pipelines, 'prod')).toEqual(['prod|ef4444'])
  })

  it('returns the matching tags sorted by name', () => {
    const pipelines = [
      pipeline('a', ['team-b|ef4444', 'other']),
      pipeline('b', ['team-a|22c55e', 'my-team'])
    ]
    expect(findMatchingTags(pipelines, 'team')).toEqual([
      'my-team',
      'team-a|22c55e',
      'team-b|ef4444'
    ])
  })

  it('does not match the color part of a tag', () => {
    expect(findMatchingTags([pipeline('a', ['prod|ef4444'])], 'ef44')).toEqual([])
  })

  it('returns no tags for an empty or blank query', () => {
    const pipelines = [pipeline('a', ['prod'])]
    expect(findMatchingTags(pipelines, '')).toEqual([])
    expect(findMatchingTags(pipelines, '   ')).toEqual([])
  })
})
