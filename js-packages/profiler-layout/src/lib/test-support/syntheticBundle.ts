// Packs a synthetic circuit into a support bundle zip, with the layout and compression of a real
// bundle: one timestamp directory, a gzip-compressed circuit profile, and entries deflated by the zip.

import type { SyntheticCircuit } from './syntheticCircuit'

const COLLECTION_DIR = '2026-11-11T12-00-00.000Z'

/** A support bundle with the profile, dataflow graph, program and logs of `circuit`. */
export async function syntheticBundle(circuit: SyntheticCircuit): Promise<Uint8Array> {
  const encode = (text: string) => new TextEncoder().encode(text)
  const logs = Array.from({ length: 5000 }, (_, i) => `2026-11-11T12:00:${i % 60}Z INFO step ${i}`)
  return zip([
    {
      name: `${COLLECTION_DIR}/circuit_profile.json.gz`,
      data: await compress(encode(JSON.stringify(circuit.profile)), 'gzip')
    },
    {
      name: `${COLLECTION_DIR}/dataflow_graph.json`,
      data: encode(JSON.stringify(circuit.dataflow))
    },
    {
      name: `${COLLECTION_DIR}/pipeline_config.json`,
      data: encode(
        JSON.stringify({ name: 'synthetic', program_code: circuit.programCode.join('\n') })
      )
    },
    { name: `${COLLECTION_DIR}/logs.txt`, data: encode(logs.join('\n')) }
  ])
}

async function compress(data: Uint8Array, format: CompressionFormat): Promise<Uint8Array> {
  const stream = new Blob([new Uint8Array(data)])
    .stream()
    .pipeThrough(new CompressionStream(format))
  return new Uint8Array(await new Response(stream).arrayBuffer())
}

const CRC_TABLE = Array.from({ length: 256 }, (_, n) => {
  let c = n
  for (let k = 0; k < 8; k++) {
    c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1
  }
  return c >>> 0
})

function crc32(data: Uint8Array): number {
  let crc = 0xffffffff
  for (const byte of data) {
    crc = CRC_TABLE[(crc ^ byte) & 0xff]! ^ (crc >>> 8)
  }
  return (crc ^ 0xffffffff) >>> 0
}

const DEFLATE = 8
const ZIP_VERSION = 20

/** A zip archive with each file deflated. Only the fields that unzip tools read are filled in. */
async function zip(files: Array<{ name: string; data: Uint8Array }>): Promise<Uint8Array> {
  const parts: Uint8Array[] = []
  const central: Uint8Array[] = []
  let offset = 0
  for (const { name, data } of files) {
    const nameBytes = new TextEncoder().encode(name)
    const packed = await compress(data, 'deflate-raw')
    const fields = { crc: crc32(data), packedSize: packed.length, size: data.length }

    const local = new DataView(new ArrayBuffer(30))
    local.setUint32(0, 0x04034b50, true)
    local.setUint16(4, ZIP_VERSION, true)
    local.setUint16(8, DEFLATE, true)
    local.setUint32(14, fields.crc, true)
    local.setUint32(18, fields.packedSize, true)
    local.setUint32(22, fields.size, true)
    local.setUint16(26, nameBytes.length, true)

    const entry = new DataView(new ArrayBuffer(46))
    entry.setUint32(0, 0x02014b50, true)
    entry.setUint16(4, ZIP_VERSION, true)
    entry.setUint16(6, ZIP_VERSION, true)
    entry.setUint16(10, DEFLATE, true)
    entry.setUint32(16, fields.crc, true)
    entry.setUint32(20, fields.packedSize, true)
    entry.setUint32(24, fields.size, true)
    entry.setUint16(28, nameBytes.length, true)
    entry.setUint32(42, offset, true)

    parts.push(new Uint8Array(local.buffer), nameBytes, packed)
    central.push(new Uint8Array(entry.buffer), nameBytes)
    offset += 30 + nameBytes.length + packed.length
  }
  const centralSize = central.reduce((size, part) => size + part.length, 0)
  const end = new DataView(new ArrayBuffer(22))
  end.setUint32(0, 0x06054b50, true)
  end.setUint16(8, files.length, true)
  end.setUint16(10, files.length, true)
  end.setUint32(12, centralSize, true)
  end.setUint32(16, offset, true)

  const blob = new Blob(
    [...parts, ...central, new Uint8Array(end.buffer)].map((p) => new Uint8Array(p))
  )
  return new Uint8Array(await blob.arrayBuffer())
}
