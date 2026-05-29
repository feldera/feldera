import type { DecodedBundle, TriagePlugin, TriageRuleResult } from 'triage-types'
import { TriageResults } from 'triage-types'

export { TriageResults }

export async function createBundle(_files: unknown): Promise<DecodedBundle> {
  return {}
}

const mockFindings: TriageRuleResult[] = [
  {
    rule: 'circuit/backpressure',
    severity: 'error',
    message:
      'Operator "join_orders_customers" is bottlenecked: input queue grew by 1.2M records over the last 5 minutes.',
    details: {
      operator: 'join_orders_customers',
      operator_id: 42,
      input_queue_growth: 1_200_000,
      window_seconds: 300,
      suggestion: 'Increase worker count or partition the input by customer_id.'
    }
  },
  {
    rule: 'storage/disk-pressure',
    severity: 'error',
    message:
      'Persistent state for materialized view "daily_summary" is approaching the disk quota.',
    details: {
      view: 'daily_summary',
      bytes_used: 18_253_611_008,
      bytes_quota: 21_474_836_480,
      utilization_pct: 85
    }
  },
  {
    rule: 'connector/lag',
    severity: 'warning',
    message: 'Kafka source "orders_topic" is lagging behind by 42,318 records.',
    details: {
      connector: 'orders_topic',
      lag_records: 42_318,
      partitions: [
        { partition: 0, lag: 12_104 },
        { partition: 1, lag: 11_088 },
        { partition: 2, lag: 19_126 }
      ]
    }
  },
  {
    rule: 'memory/operator-high',
    severity: 'warning',
    message:
      'Aggregate operator "window_avg_price" is using 3.4 GiB of memory — above the 2 GiB warning threshold.',
    details: {
      operator: 'window_avg_price',
      memory_bytes: 3_650_722_201,
      threshold_bytes: 2_147_483_648
    }
  },
  {
    rule: 'sql/unused-index',
    severity: 'info',
    message: 'Index "customers_email_idx" was declared but no query in this pipeline uses it.',
    details: {
      index: 'customers_email_idx',
      table: 'customers'
    }
  },
  {
    rule: 'pipeline/idle-worker',
    severity: 'info',
    message: 'Worker 3 has been idle for 87% of the last sample window.',
    details: {
      worker: 3,
      idle_pct: 87,
      window_seconds: 60
    }
  }
]

const mockPlugin: TriagePlugin = {
  name: 'mock-triage-plugin',
  triage(_bundle, results) {
    results.push(mockFindings)
  }
}

const plugins: TriagePlugin[] = [mockPlugin]
export default plugins
