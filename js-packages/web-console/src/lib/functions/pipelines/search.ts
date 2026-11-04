import { partition } from 'common-lib/array'
import { matchesSubstring } from 'common-lib/string'
import { tagDisplayName } from './tags.ts'

type Searchable = { name: string; tags: string[] }

const hasTagMatching = (pipeline: Searchable, query: string) =>
  pipeline.tags.some((tag) => matchesSubstring(tagDisplayName(tag), query))

/**
 * Whether a pipeline matches the search: its name or one of its tag names contains `query`,
 * and it carries a tag with the name of each tag in `tagFilters`.
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
  return matchesSubstring(pipeline.name, trimmed) || hasTagMatching(pipeline, trimmed)
}

/** Move the pipelines whose name contains `query` to the front. Keeps the order inside each group. */
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
