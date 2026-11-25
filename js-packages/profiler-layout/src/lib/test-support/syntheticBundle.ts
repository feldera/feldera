// Packs a synthetic circuit into a support bundle zip with the layout and compression of a real
// bundle. The zip has one timestamp directory, a gzip-compressed circuit profile, and deflated entries.

import { gzipSync, strToU8, zipSync } from 'fflate'
import type { SyntheticCircuit } from './syntheticCircuit'

const COLLECTION_DIR = '2026-11-11T12-00-00.000Z'

/** Builds a support bundle with the profile, dataflow graph, program and logs of `circuit`. */
export function syntheticBundle(circuit: SyntheticCircuit): Uint8Array {
  const logs = Array.from({ length: 5000 }, (_, i) => `2026-11-11T12:00:${i % 60}Z INFO step ${i}`)
  const programConfig = { name: 'synthetic', program_code: circuit.programCode.join('\n') }
  return zipSync({
    [`${COLLECTION_DIR}/circuit_profile.json.gz`]: gzipSync(
      strToU8(JSON.stringify(circuit.profile))
    ),
    [`${COLLECTION_DIR}/dataflow_graph.json`]: strToU8(JSON.stringify(circuit.dataflow)),
    [`${COLLECTION_DIR}/pipeline_config.json`]: strToU8(JSON.stringify(programConfig)),
    [`${COLLECTION_DIR}/logs.txt`]: strToU8(logs.join('\n'))
  })
}
