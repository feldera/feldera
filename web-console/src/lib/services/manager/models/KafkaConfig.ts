/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

/**
 * Configuration for accessing a Kafka service.
 */
export type KafkaConfig = {
  /**
   * List of bootstrap servers
   */
  bootstrap_servers: Array<string>
  /**
   * Additional Kafka options
   */
  options: Record<string, string>
}
