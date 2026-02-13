import type { ZipItem } from 'but-unzip'

export type TriageSeverity = 'info' | 'warning' | 'error'

export interface TriageResult {
  rule: string
  severity: TriageSeverity
  message: string
  pipelineName?: string
  details?: Record<string, unknown>
}

export interface TriagePlugin {
  name: string
  triage: (files: ZipItem[]) => Promise<TriageResult[]>
}
