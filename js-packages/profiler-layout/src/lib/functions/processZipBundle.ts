import { unzip, type ZipItem } from 'but-unzip'
import type { Dataflow, JsonProfiles } from 'profiler-lib'
import sortOn from 'sort-on'
import { groupBy } from './array'
import type { GlobalMetrics } from './globalMetrics'

const circuitProfileRegex = /circuit_profile\.json$/
const dataflowGraphRegex = /dataflow_graph\.json$/
const pipelineConfigRegex = /pipeline_config\.json$/
const logsRegex = /logs\.txt$/
const statsRegex = /stats\.json$/

// Any of these files makes a collection worth opening. A bundle missing the circuit profile
// still carries program code, config, logs and stats, all of which the viewer can display.
const usefulFileRegexes = [
  circuitProfileRegex,
  dataflowGraphRegex,
  pipelineConfigRegex,
  logsRegex,
  statsRegex
]

// New bundle layout (since support-bundle metadata_version 1): each collection
// lives under a directory named after its timestamp, e.g.
// "2026-05-25T12-34-56.789Z/circuit_profile.json". Collisions get a "-N" suffix
// on the directory. The directory may be placed in a nested directory
// (e.g. "support/2026-05-25T12-34-56.789Z/circuit_profile.json"),
// so we match the timestamp segment anywhere in the path, not only at the start.
const collectionDirRegex = /(?:^|\/)(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}(?:\.\d+)?Z(?:-\d+)?)\//

// Legacy layout: files were named "<ISO_TIMESTAMP>_<file>", optionally inside
// a sub-path, e.g. "2026-01-19T12:55:54.152834443+00:00_logs.txt".
const legacyTimestampPrefixRegex = /(?:^|\/)(\d{4}-\d{2}-\d{2}T[^/_]+)_/

/** Group key identifying one collection. Empty string means "not a collection file". */
function collectionKey(filename: string): string {
  return (
    filename.match(collectionDirRegex)?.[1] ?? filename.match(legacyTimestampPrefixRegex)?.[1] ?? ''
  )
}

/** Parse a collection key back into a Date for sorting and display. */
function collectionDate(key: string): Date {
  // New keys use '-' between hours/minutes/seconds; JavaScript's Date parser
  // wants ':', so rewrite the time portion. The trailing "-N" collision suffix
  // (if any) is dropped — it carries no temporal information.
  const m = key.match(/^(\d{4}-\d{2}-\d{2})T(\d{2})-(\d{2})-(\d{2})(\.\d+)?Z/)
  if (m) {
    return new Date(`${m[1]}T${m[2]}:${m[3]}:${m[4]}${m[5] ?? ''}Z`)
  }
  return new Date(key)
}

export interface ProcessedProfile {
  /** Parsed `circuit_profile.json` */
  profile?: JsonProfiles
  dataflow?: Dataflow
  sources?: string[]
  logText?: string
  pipelineName?: string
  /** Cumulative pipeline-wide metrics from `stats.json`, when the bundle includes them. */
  globalMetrics?: GlobalMetrics
  /** Pipeline runtime configuration (`runtime_config` from `pipeline_config.json`), when the
   *  bundle includes it. */
  runtimeConfig?: unknown
}

/**
 * Unzip a bundle and extract every readable profile with its files.
 * This function only unzips once - store the result for efficient timestamp switching.
 * A profile is readable when it holds any recognised file (circuit profile, dataflow graph,
 * pipeline config, logs or stats).
 * @param zipData Raw zip file data
 * @returns Array of [timestamp, files] tuples sorted by timestamp (oldest first)
 * @throws Error if the zip is invalid
 */
export function getSuitableProfiles(zipData: Uint8Array): [Date, ZipItem[]][] {
  let profileFiles: ZipItem[]
  try {
    profileFiles = unzip(zipData)
  } catch (error) {
    throw new Error(
      `Failed to unzip bundle: ${error instanceof Error ? error.message : String(error)}`
    )
  }
  return selectProfiles(profileFiles)
}

/**
 * Group already-unzipped bundle files into readable profiles, sorted oldest-first.
 * A collection is kept when its timestamp directory holds any recognised file; the circuit
 * profile is not required.
 */
export function selectProfiles(files: ZipItem[]): [Date, ZipItem[]][] {
  const profiles = groupBy(files, (file) => collectionKey(file.filename)).filter(
    (group) =>
      group[0] &&
      group[1].some((file) => usefulFileRegexes.some((regex) => regex.test(file.filename)))
  )

  return sortOn(
    profiles.map(([key, files]) => [collectionDate(key), files] as [Date, ZipItem[]]),
    (p) => p[0]
  )
}

/**
 * Process a specific set of profile files (already unzipped) into structured data.
 * Use this with the files from getSuitableProfiles() for efficient processing.
 * @param files Array of ZipItem files for a specific profile timestamp
 * @returns Processed profile data
 */
export async function processProfileFiles(files: ZipItem[]): Promise<ProcessedProfile> {
  const decoder = new TextDecoder()

  // The circuit profile is optional: a bundle without it still yields config, logs and stats.
  const profileFile = files.find((file) => circuitProfileRegex.test(file.filename))
  let profile: JsonProfiles | undefined
  if (profileFile) {
    profile = JSON.parse(decoder.decode(await profileFile.read())) as JsonProfiles
  }

  const dataflowFile = files.find((file) => dataflowGraphRegex.test(file.filename))
  let dataflow: Dataflow | undefined
  if (dataflowFile) {
    dataflow = JSON.parse(decoder.decode(await dataflowFile.read()))
  }

  const configFile = files.find((file) => pipelineConfigRegex.test(file.filename))
  let sources: string[] | undefined
  let pipelineName: string | undefined
  let runtimeConfig: unknown
  if (configFile) {
    const pipelineConfig = JSON.parse(decoder.decode(await configFile.read())) as unknown as {
      program_code: string
      name?: string
      runtime_config?: unknown
    }
    sources = pipelineConfig.program_code.split('\n')
    pipelineName = pipelineConfig.name || undefined
    runtimeConfig = pipelineConfig.runtime_config ?? undefined
  }

  const logsFile = files.find((file) => logsRegex.test(file.filename))
  let logText: string | undefined
  if (logsFile) {
    logText = decoder.decode(await logsFile.read())
  }

  // `stats.json` is the `/stats` response; the overview tile only needs its `global_metrics`.
  // A malformed or missing file leaves `globalMetrics` undefined rather than failing the load,
  // since the stats are supplementary to the profile.
  const statsFile = files.find((file) => statsRegex.test(file.filename))
  let globalMetrics: GlobalMetrics | undefined
  if (statsFile) {
    try {
      const stats = JSON.parse(decoder.decode(await statsFile.read())) as {
        global_metrics?: GlobalMetrics
      }
      globalMetrics = stats.global_metrics
    } catch {
      globalMetrics = undefined
    }
  }

  return {
    profile,
    dataflow,
    sources,
    logText,
    pipelineName,
    globalMetrics,
    runtimeConfig
  }
}
