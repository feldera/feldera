/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { DeltaTableReaderConfig } from './DeltaTableReaderConfig'
import type { DeltaTableWriterConfig } from './DeltaTableWriterConfig'
import type { FileInputConfig } from './FileInputConfig'
import type { FileOutputConfig } from './FileOutputConfig'
import type { KafkaInputConfig } from './KafkaInputConfig'
import type { KafkaOutputConfig } from './KafkaOutputConfig'
import type { S3InputConfig } from './S3InputConfig'
import type { UrlInputConfig } from './UrlInputConfig'
/**
 * Transport-specific endpoint configuration passed to
 * `crate::OutputTransport::new_endpoint`
 * and `crate::InputTransport::new_endpoint`.
 */
export type TransportConfig =
  | {
      config: FileInputConfig
      name: TransportConfig.name.FILE_INPUT
    }
  | {
      config: FileOutputConfig
      name: TransportConfig.name.FILE_OUTPUT
    }
  | {
      config: KafkaInputConfig
      name: TransportConfig.name.KAFKA_INPUT
    }
  | {
      config: KafkaOutputConfig
      name: TransportConfig.name.KAFKA_OUTPUT
    }
  | {
      config: UrlInputConfig
      name: TransportConfig.name.URL_INPUT
    }
  | {
      config: S3InputConfig
      name: TransportConfig.name.S3_INPUT
    }
  | {
      config: DeltaTableReaderConfig
      name: TransportConfig.name.DELTA_TABLE_INPUT
    }
  | {
      config: DeltaTableWriterConfig
      name: TransportConfig.name.DELTA_TABLE_OUTPUT
    }
export namespace TransportConfig {
  export enum name {
    FILE_INPUT = 'file_input',
    FILE_OUTPUT = 'file_output',
    KAFKA_INPUT = 'kafka_input',
    KAFKA_OUTPUT = 'kafka_output',
    URL_INPUT = 'url_input',
    S3_INPUT = 's3_input',
    DELTA_TABLE_INPUT = 'delta_table_input',
    DELTA_TABLE_OUTPUT = 'delta_table_output',
    HTTP_INPUT = 'http_input',
    HTTP_OUTPUT = 'http_output'
  }
}
