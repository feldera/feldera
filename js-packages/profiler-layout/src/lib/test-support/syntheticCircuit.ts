// A deterministic generator for a large circuit profile and its dataflow graph, for the performance
// suite. The sample bundles hold about a hundred operators, and real circuits can hold thousands.
// The shape follows a real profile: regions nested three levels deep, edges that chain the operators
// of a region and join regions together, metrics on every worker, and source positions for part of
// the operators.

import type { Dataflow, JsonProfiles } from 'profiler-lib'

export interface SyntheticCircuitOptions {
  /** The same seed gives the same circuit. */
  seed: number
  /** How many operators and regions the top level holds. The total grows about linearly with it. */
  topLevelItems: number
  workers: number
}

export interface SyntheticCircuit {
  profile: JsonProfiles
  dataflow: Dataflow
  /** The SQL program, one string for each line. */
  programCode: string[]
  operatorCount: number
  regionCount: number
  /** The top-level region with the most direct children. */
  largestRegionId: string
  /** The first operator. Most of the circuit is downstream of it, so a hover on it colors the most
   *  edges. */
  sourceOperatorId: string
}

/** The chance that an item is a region, for each nesting level. Deeper levels have fewer regions. */
const REGION_CHANCE = [0.4, 0.3, 0.15]
const MIN_REGION_ITEMS = 3
const MAX_REGION_ITEMS = 10
const WORKER_SKEW_MIN = 0.5

const OPERATOR_NAMES = [
  'Map',
  'MapIndexedZSet',
  'Filter',
  'Apply',
  'JoinTrace',
  'Accumulator',
  'AccumulateOutput',
  'AccumulateTraceAppend',
  'ShardedAccumulatorSender',
  'ShardedAccumulatorReceiver',
  'ExchangeSender',
  'ExchangeReceiver',
  'Plus',
  'Recorder',
  'PartitionedRollingAggregate'
]
const REGION_NAMES = [
  'shard_accumulate_trace',
  'accumulate_integrate_trace',
  'left_join',
  'join',
  'saturate',
  'partitioned_tree_aggregate',
  'waterline'
]

const METRICS: JsonProfiles['metrics'] = [
  {
    name: 'invocations_count',
    category: 'Time',
    advanced: true,
    description: 'Number of times the operator has been invoked.'
  },
  {
    name: 'runtime_seconds',
    category: 'Time',
    advanced: false,
    description: 'Total time spent evaluating the operator.'
  },
  {
    name: 'runtime_percent',
    category: 'Time',
    advanced: false,
    description: 'Percentage of time spent evaluating the operator.'
  },
  {
    name: 'nonblocking_percent',
    category: 'Time',
    advanced: true,
    description: 'Fraction of operator runtime spent running.'
  },
  {
    name: 'used_memory_bytes',
    category: 'State',
    advanced: true,
    description: 'Bytes used by the in-memory state of the operator.'
  },
  {
    name: 'allocated_memory_bytes',
    category: 'State',
    advanced: false,
    description: 'Total bytes reserved by the operator state.'
  },
  {
    name: 'state_records_count',
    category: 'State',
    advanced: false,
    description: 'Number of records stored in operator state.'
  },
  {
    name: 'output_batches_stats',
    category: 'Outputs',
    advanced: false,
    description: 'Distribution of output batch sizes.'
  }
] as unknown as JsonProfiles['metrics']

/** mulberry32: a small seeded generator of numbers in [0, 1). */
function seededRandom(seed: number) {
  let state = seed >>> 0
  return () => {
    state = (state + 0x6d2b79f5) >>> 0
    let t = state
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

type JsonItem =
  | { Simple: { id: string; label: string } }
  | { Cluster: { id: string; label: string; nodes: JsonItem[] } }

interface Operator {
  id: string
  /** The top-level item the operator is in, which is also its SQL statement. */
  statement: number
  persistentId?: string
  isStateful: boolean
  hasBatchStats: boolean
  /** The cost of the operator. The metrics of all workers scale from it. */
  weight: number
}

export function syntheticCircuit({
  seed,
  topLevelItems,
  workers
}: SyntheticCircuitOptions): SyntheticCircuit {
  const random = seededRandom(seed)
  const integer = (min: number, max: number) => min + Math.floor(random() * (max - min + 1))
  const pick = <T>(items: readonly T[]) => items[Math.floor(random() * items.length)]!

  const operators: Operator[] = []
  const edges: JsonProfiles['graph']['edges'] = []
  let regionCount = 0
  let largestRegion = { id: '', size: 0 }

  const edge = (from: string, to: string) =>
    edges.push({ from_node: from, to_node: to, from_cluster: false, to_cluster: false } as never)

  /** One operator. It takes input from the previous operator of its region, and sometimes also from
   *  an earlier operator anywhere in the circuit, like a join. */
  const addOperator = (statement: number, previousInRegion: string | undefined): JsonItem => {
    const id = `nn${operators.length}`
    if (previousInRegion !== undefined && random() < 0.85) {
      edge(previousInRegion, id)
    } else if (operators.length > 0) {
      edge(pick(operators).id, id)
    }
    if (operators.length > 1 && random() < 0.15) {
      const recent = operators.slice(-200)
      edge(pick(recent).id, id)
    }
    operators.push({
      id,
      statement,
      persistentId: random() < 0.5 ? `pid-${id}` : undefined,
      isStateful: random() < 0.35,
      hasBatchStats: random() < 0.4,
      weight: Math.exp(random() * 8)
    })
    const file = pick(['c/f/s/lib.rs', 's/o/d/input.rs', 's/typed_batch.rs'])
    return {
      Simple: {
        id,
        label: `${pick(OPERATOR_NAMES)} @ ${file}:${integer(1, 900)}:${integer(1, 200)}`
      }
    }
  }

  const addRegion = (statement: number, depth: number): JsonItem => {
    const id = `n_r${regionCount++}`
    const nodes = addItems(statement, depth + 1, integer(MIN_REGION_ITEMS, MAX_REGION_ITEMS))
    if (depth === 0 && nodes.length > largestRegion.size) {
      largestRegion = { id, size: nodes.length }
    }
    return {
      Cluster: { id, label: `${pick(REGION_NAMES)} @ s/o/d/region.rs:${integer(1, 900)}:14`, nodes }
    }
  }

  /** `count` items at `depth`, each an operator or a region. Regions are not created below the
   *  deepest level that `REGION_CHANCE` gives. */
  function addItems(statement: number, depth: number, count: number): JsonItem[] {
    const items: JsonItem[] = []
    let previous: string | undefined
    for (let i = 0; i < count; i++) {
      const isRegion = random() < (REGION_CHANCE[depth] ?? 0)
      items.push(isRegion ? addRegion(statement, depth) : addOperator(statement, previous))
      previous = isRegion ? undefined : operators[operators.length - 1]!.id
    }
    return items
  }

  const topLevel: JsonItem[] = []
  for (let statement = 0; statement < topLevelItems; statement++) {
    topLevel.push(...addItems(statement, 0, 1))
  }

  const workerSkews = Array.from({ length: workers }, () => WORKER_SKEW_MIN + random())
  const worker_profiles = workerSkews.map((skew) => ({
    metadata: Object.fromEntries([
      ['n', rootMetrics(skew)],
      ...operators.map((op) => [op.id, operatorMetrics(op, skew * (0.8 + 0.4 * random()))])
    ])
  }))

  const { programCode, dataflow } = sqlProgram(operators, topLevelItems)
  return {
    profile: {
      metrics: METRICS,
      worker_profiles,
      graph: { nodes: { id: 'n', label: 'root', nodes: topLevel }, edges }
    } as unknown as JsonProfiles,
    dataflow,
    programCode,
    operatorCount: operators.length,
    regionCount,
    largestRegionId: largestRegion.id,
    sourceOperatorId: operators[0]!.id
  }
}

const count = (value: number) => ({ type: 'count', value: Math.round(value) })
const bytes = (value: number) => ({ type: 'bytes', value: Math.round(value) })
const duration = (seconds: number) => ({
  type: 'duration',
  value: { secs: Math.floor(seconds), nanos: Math.round((seconds % 1) * 1e9) }
})
const percent = (numerator: number, denominator: number) => ({
  type: 'percent',
  value: { numerator: Math.round(numerator), denominator: Math.round(denominator) }
})
const metric = (metric_id: string, value: unknown) => ({ metric_id, value })

function rootMetrics(skew: number) {
  return [
    metric('circuit_runtime_seconds', duration(120 * skew)),
    metric('allocated_memory_bytes', bytes(6e7 * skew))
  ]
}

/** The metrics of one operator on one worker. Their mix follows a real profile: every operator has
 *  the time metrics, and part of them have state and batch statistics. */
function operatorMetrics(op: Operator, skew: number) {
  const load = op.weight * skew
  const entries = [
    metric('invocations_count', count(4260)),
    metric('runtime_seconds', duration(load / 1000)),
    metric('runtime_percent', percent(load * 1000, 3.2e9)),
    metric('nonblocking_percent', percent(load * 900, load * 1000))
  ]
  if (op.persistentId !== undefined) {
    entries.push(metric('persistent_id', { type: 'string', value: op.persistentId }))
  }
  if (op.isStateful) {
    entries.push(
      metric('used_memory_bytes', bytes(load * 4096)),
      metric('allocated_memory_bytes', bytes(load * 5120)),
      metric('state_records_count', count(load * 30))
    )
  }
  if (op.hasBatchStats) {
    const records = load * 15
    entries.push(
      metric('output_batches_stats', {
        avg_records_count: count(records / 4260),
        batches_count: count(4260),
        max_records_count: count(records / 100),
        min_records_count: count(0),
        total_records_count: count(records)
      })
    )
  }
  return entries
}

/** A SQL program with one statement for each top-level item, and the dataflow graph that maps the
 *  operators with a persistent id to positions in it. The first operator of a statement also names
 *  its table or view, which search finds by name. */
function sqlProgram(operators: Operator[], statements: number) {
  const LINES_PER_STATEMENT = 4
  const programCode: string[] = []
  for (let s = 0; s < statements; s++) {
    programCode.push(
      `CREATE VIEW v${s} AS`,
      `  SELECT a.id, SUM(a.amount) AS total FROM v${Math.max(0, s - 1)} a`,
      `  JOIN t${s % 7} b ON a.id = b.id`,
      `  GROUP BY a.id;`
    )
  }
  const mir: Dataflow['mir'] = {}
  const named = new Set<number>()
  for (const op of operators) {
    if (op.persistentId === undefined) {
      continue
    }
    const line = op.statement * LINES_PER_STATEMENT + 1 + (op.id.length % LINES_PER_STATEMENT)
    const isFirstOfStatement = !named.has(op.statement)
    named.add(op.statement)
    mir[`s${op.id}`] = {
      operation: 'map_index',
      table: null,
      view: isFirstOfStatement ? `v${op.statement}` : null,
      inputs: [],
      calcite: {},
      persistent_id: op.persistentId,
      positions: [
        { start_line_number: line, start_column: 3, end_line_number: line, end_column: 30 }
      ]
    } as never
  }
  return { programCode, dataflow: { calcite_plan: {}, mir } as Dataflow }
}
