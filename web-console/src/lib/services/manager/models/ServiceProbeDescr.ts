/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ServiceProbeId } from './ServiceProbeId'
import type { ServiceProbeRequest } from './ServiceProbeRequest'
import type { ServiceProbeResponse } from './ServiceProbeResponse'
import type { ServiceProbeStatus } from './ServiceProbeStatus'
import type { ServiceProbeType } from './ServiceProbeType'
/**
 * Service probe descriptor.
 */
export type ServiceProbeDescr = {
  created_at: string
  finished_at?: string | null
  probe_type: ServiceProbeType
  request: ServiceProbeRequest
  response?: ServiceProbeResponse | null
  service_probe_id: ServiceProbeId
  started_at?: string | null
  status: ServiceProbeStatus
}
