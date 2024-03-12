/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { KafkaService } from './KafkaService'
/**
 * Service configuration for the API
 *
 * A Service is an API object, with as one of its properties its config.
 * The config is a variant of this enumeration, and is stored serialized
 * in the database.
 *
 * How a service configuration is applied can vary by connector, e.g., some
 * might have options that are mutually exclusive whereas others might be
 * defaults that can be overriden.
 */
export type ServiceConfig = {
  kafka: KafkaService
}
