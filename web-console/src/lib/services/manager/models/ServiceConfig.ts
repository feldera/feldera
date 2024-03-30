/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { KafkaService } from './KafkaService'
/**
 * Configuration for a Service, which typically includes how to establish a
 * connection (e.g., hostname, port) and authenticate (e.g., credentials).
 *
 * This configuration can be used to easily derive connectors for the service
 * as well as probe it for information.
 */
export type ServiceConfig = {
  kafka: KafkaService
}
