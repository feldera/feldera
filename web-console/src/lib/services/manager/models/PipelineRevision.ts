/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ConnectorDescr } from './ConnectorDescr'
import type { PipelineConfig } from './PipelineConfig'
import type { PipelineDescr } from './PipelineDescr'
import type { ProgramDescr } from './ProgramDescr'
import type { Revision } from './Revision'
/**
 * A pipeline revision is a versioned, immutable configuration struct that
 * contains all information necessary to run a pipeline.
 */
export type PipelineRevision = {
  config: PipelineConfig
  /**
   * The versioned connectors.
   */
  connectors: Array<ConnectorDescr>
  pipeline: PipelineDescr
  program: ProgramDescr
  revision: Revision
}
