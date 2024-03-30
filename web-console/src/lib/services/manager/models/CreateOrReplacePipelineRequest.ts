/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { AttachedConnector } from './AttachedConnector'
import type { RuntimeConfig } from './RuntimeConfig'
/**
 * Request to create or replace an existing pipeline.
 */
export type CreateOrReplacePipelineRequest = {
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
   * Name of the program to create a pipeline for.
   */
  program_name?: string | null
}
