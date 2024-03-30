/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { AttachedConnector } from './AttachedConnector'
import type { PipelineId } from './PipelineId'
import type { RuntimeConfig } from './RuntimeConfig'
import type { Version } from './Version'
/**
 * Pipeline descriptor.
 */
export type PipelineDescr = {
  attached_connectors: Array<AttachedConnector>
  config: RuntimeConfig
  description: string
  name: string
  pipeline_id: PipelineId
  program_name?: string | null
  version: Version
}
