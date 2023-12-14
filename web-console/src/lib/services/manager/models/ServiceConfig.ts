/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { KafkaConfig } from './KafkaConfig'
import type { MysqlConfig } from './MysqlConfig'

/**
 * A service's configuration.
 */
export type ServiceConfig =
  | {
      mysql: MysqlConfig
    }
  | {
      kafka: KafkaConfig
    }
