/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * Fault tolerance configuration for Kafka input connector.
 */
export type KafkaInputFtConfig = {
  /**
   * Options passed to `rdkafka` for consumers only, as documented at
   * [`librdkafka`
   * options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
   *
   * These options override `kafka_options` for consumers, and may be empty.
   */
  consumer_options?: Record<string, string>
  /**
   * If this is true or unset, then the connector will create missing index
   * topics as needed.  If this is false, then a missing index topic is a
   * fatal error.
   */
  create_missing_index?: boolean | null
  /**
   * Suffix to append to each data topic name, to give the name of a topic
   * that the connector uses for recording the division of the corresponding
   * data topic into steps.  Defaults to `_input-index`.
   *
   * An index topic must have the same number of partitions as its
   * corresponding data topic.
   *
   * If two or more fault-tolerant Kafka endpoints read from overlapping sets
   * of topics, they must specify different `index_suffix` values.
   */
  index_suffix?: string | null
  /**
   * Maximum number of bytes in a step.  Any individual message bigger than
   * this will be given a step of its own.
   */
  max_step_bytes?: number | null
  /**
   * Maximum number of messages in a step.
   */
  max_step_messages?: number | null
  /**
   * Options passed to `rdkafka` for producers only, as documented at
   * [`librdkafka`
   * options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
   *
   * These options override `kafka_options` for producers, and may be empty.
   */
  producer_options?: Record<string, string>
}
