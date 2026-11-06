import { partition } from 'common-lib/array'
import { matchesSubstring } from 'common-lib/string'
import { tagDisplayName } from './tags.ts'

type Searchable = { name: string; tags: string[] }

/**
 * Whether a pipeline matches the search. A pipeline matches if both are true:
 * - its name or one of its tag names contains `query`,
 * - it has a tag with the name of each tag in `tagFilters`. The tag colors can differ.
 */
export const matchesPipelineSearch = (
  pipeline: Searchable,
  query: string,
  tagFilters: readonly string[]
) => {
  const pipelineTagNames = pipeline.tags.map(tagDisplayName)
  if (!tagFilters.every((filter) => pipelineTagNames.includes(tagDisplayName(filter)))) {
    return false
  }
  const trimmed = query.trim()
  return (
    matchesSubstring(pipeline.name, trimmed) ||
    pipelineTagNames.some((name) => matchesSubstring(name, trimmed))
  )
}

/** A stable sort that moves the pipelines whose name contains `query` to the front. */
export const nameMatchesFirst = <P extends Searchable>(
  pipelines: readonly P[],
  query: string
): readonly P[] => {
  const trimmed = query.trim()
  if (!trimmed) {
    return pipelines
  }
  return partition([...pipelines], (pipeline) => matchesSubstring(pipeline.name, trimmed)).flat()
}

/**
 * The tags of `pipelines` whose name contains `query`, one per name, sorted by name.
 * An empty query matches no tags.
 */
export const findMatchingTags = (pipelines: Searchable[], query: string): string[] => {
  const trimmed = query.trim()
  if (!trimmed) {
    return []
  }
  const tagsByName = new Map<string, string>()
  for (const tag of pipelines.flatMap((pipeline) => pipeline.tags)) {
    const name = tagDisplayName(tag)
    if (!tagsByName.has(name) && matchesSubstring(name, trimmed)) {
      tagsByName.set(name, tag)
    }
  }
  return [...tagsByName.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([, tag]) => tag)
}
