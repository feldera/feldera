import { unzip, type ZipItem } from 'but-unzip'
import type { Dataflow, JsonProfiles } from 'profiler-lib'
import sortOn from 'sort-on'
import { groupBy } from './array'

const circuitProfileRegex = /circuit_profile\.json$/
const dataflowGraphRegex = /dataflow_graph\.json$/
const pipelineConfigRegex = /pipeline_config\.json$/

export interface ProcessedProfile {
  profile: JsonProfiles
  dataflow?: Dataflow
  sources?: string[]
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

  const profileTimestamps = groupBy(
    profileFiles,
    // group by file timestamp; all files in a bundle are named with names like TIMESTAMP_FILENAME
    // where TIMESTAMP is an ISO 8601 timestamp
    // Examples: "2026-01-19T12:55:54.152834443+00:00_logs.txt", "some/path/2026-01-19T12:55:54.152834443+00:00_logs.txt"
    (file) => file.filename.match(/(?:^|\/)(\d{4}-\d{2}-\d{2}T[^/_]+)_/)?.[1] ?? ''
  ).filter(
    (group) => group[0] && group[1].some((file) => circuitProfileRegex.test(file.filename))
    // Pipeline config and dataflow graph are not required
  )

  return sortOn(
    profileTimestamps.map(
      ([timestamp, files]) => [new Date(timestamp), files] as [Date, ZipItem[]]
    ),
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
  if (configFile) {
    const pipelineConfig = JSON.parse(decoder.decode(await configFile.read())) as unknown as {
      program_code: string
    }
    sources = pipelineConfig.program_code.split('\n')
  }

  return {
    profile,
    dataflow,
    sources
  }
}
