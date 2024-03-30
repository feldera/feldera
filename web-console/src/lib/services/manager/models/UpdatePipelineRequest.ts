/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { AttachedConnector } from './AttachedConnector'
import type { RuntimeConfig } from './RuntimeConfig'
/**
 * Request to update an existing pipeline.
 */
export type UpdatePipelineRequest = {
  config?: RuntimeConfig | null
  /**
   * Attached connectors.
   *
   * - If absent, existing connectors will be kept unmodified.
   *
   * - If present all existing connectors will be replaced with the new
   * specified list.
   */
  connectors?: Array<AttachedConnector> | null
  /**
   * New pipeline description.
   */
  description: string
  /**
   * New pipeline name.
   */
  name: string
  /**
   * New program to create a pipeline for. If absent, program will be set to
   * NULL.
   */
  program_name?: string | null
}
