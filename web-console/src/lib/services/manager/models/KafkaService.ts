/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * Configuration for accessing a Kafka service.
 */
export type KafkaService = {
  /**
   * List of bootstrap servers, each formatted as hostname:port (e.g.,
   * "example.com:1234"). It will be used to set the bootstrap.servers
   * Kafka option.
   */
  bootstrap_servers: Array<string>
  /**
   * Additional Kafka options.
   *
   * Should not contain the bootstrap.servers key
   * as it is passed explicitly via its field.
   *
   * These options will likely encompass things
   * like SSL and authentication configuration.
   */
  options: Record<string, string>
}
