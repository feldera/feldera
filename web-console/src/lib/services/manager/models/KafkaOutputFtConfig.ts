/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * Fault tolerance configuration for Kafka output connector.
 */
export type KafkaOutputFtConfig = {
  /**
   * Options passed to `rdkafka` for consumers only, as documented at
   * [`librdkafka`
   * options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
   *
   * These options override `kafka_options` for consumers, and may be empty.
   */
  consumer_options?: Record<string, string>
  /**
   * Options passed to `rdkafka` for producers only, as documented at
   * [`librdkafka`
   * options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
   *
   * These options override `kafka_options` for producers, and may be empty.
   */
  producer_options?: Record<string, string>
}
