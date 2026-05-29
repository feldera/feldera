import { unzip, type ZipItem } from 'but-unzip'
import type { Dataflow, JsonProfiles } from 'profiler-lib'
import sortOn from 'sort-on'
import { groupBy } from './array'

const circuitProfileRegex = /circuit_profile\.json$/
const dataflowGraphRegex = /dataflow_graph\.json$/
const pipelineConfigRegex = /pipeline_config\.json$/
const logsRegex = /logs\.txt$/

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
  profile: JsonProfiles
  dataflow?: Dataflow
  sources?: string[]
  logText?: string
  pipelineName?: string
}

/**
 * Unzip a bundle and extract all suitable profiles with their files.
 * This function only unzips once - store the result for efficient timestamp switching.
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

  const profileTimestamps = groupBy(profileFiles, (file) => collectionKey(file.filename)).filter(
    (group) => group[0] && group[1].some((file) => circuitProfileRegex.test(file.filename))
    // Pipeline config and dataflow graph are not required
  )

  return sortOn(
    profileTimestamps.map(([key, files]) => [collectionDate(key), files] as [Date, ZipItem[]]),
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

  // Read and parse the three required files
  const profileFile = files.find((file) => circuitProfileRegex.test(file.filename))!
  const profile = JSON.parse(decoder.decode(await profileFile.read())) as JsonProfiles

  const dataflowFile = files.find((file) => dataflowGraphRegex.test(file.filename))
  let dataflow: Dataflow | undefined
  if (dataflowFile) {
    dataflow = JSON.parse(decoder.decode(await dataflowFile.read()))
  }

  const configFile = files.find((file) => pipelineConfigRegex.test(file.filename))
  let sources: string[] | undefined
  let pipelineName: string | undefined
  if (configFile) {
    const pipelineConfig = JSON.parse(decoder.decode(await configFile.read())) as unknown as {
      program_code: string
      name?: string
    }
    sources = pipelineConfig.program_code.split('\n')
    pipelineName = pipelineConfig.name || undefined
  }

  const logsFile = files.find((file) => logsRegex.test(file.filename))
  let logText: string | undefined
  if (logsFile) {
    logText = decoder.decode(await logsFile.read())
  }

  return {
    profile,
    dataflow,
    sources,
    logText,
    pipelineName
  }
}
