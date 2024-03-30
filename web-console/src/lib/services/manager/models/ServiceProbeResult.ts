/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * Enumeration of all possible service probe success responses.
 */
export type ServiceProbeResult =
  | 'connected'
  | {
      /**
       * The names of all Kafka topics of the service.
       */
      kafka_topics: Array<string>
    }
