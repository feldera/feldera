/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { PipelineDescr } from './PipelineDescr'
import type { PipelineRuntimeState } from './PipelineRuntimeState'
/**
 * State of a pipeline, including static configuration
 * and runtime status.
 */
export type Pipeline = {
  descriptor: PipelineDescr
  state: PipelineRuntimeState
}
