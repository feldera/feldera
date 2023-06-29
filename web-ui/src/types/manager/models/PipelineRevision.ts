/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ConnectorDescr } from './ConnectorDescr'
import type { PipelineDescr } from './PipelineDescr'
import type { ProgramDescr } from './ProgramDescr'
import type { Revision } from './Revision'

/**
 * A pipeline revision is a versioned, immutable configuration struct that
 * contains all information necessary to run a pipeline.
 */
export type PipelineRevision = {
  /**
   * The versioned SQL code.
   */
  code: string
  /**
   * The generated TOML config for the pipeline.
   */
  config: string
  /**
   * The versioned connectors.
   */
  connectors: Array<ConnectorDescr>
  pipeline: PipelineDescr
  program: ProgramDescr
  revision: Revision
}
