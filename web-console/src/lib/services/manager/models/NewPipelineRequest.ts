/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { AttachedConnector } from './AttachedConnector'
import type { RuntimeConfig } from './RuntimeConfig'
/**
 * Request to create a new pipeline.
 */
export type NewPipelineRequest = {
  config: RuntimeConfig
  /**
   * Attached connectors.
   */
  connectors?: Array<AttachedConnector> | null
  /**
   * Pipeline description.
   */
  description: string
  /**
   * Unique pipeline name.
   */
  name: string
  /**
   * Name of the program to create a pipeline for.
   */
  program_name?: string | null
}
