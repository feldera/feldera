/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { AwsCredentials } from './AwsCredentials'
import type { ConsumeStrategy } from './ConsumeStrategy'
import type { ReadStrategy } from './ReadStrategy'
/**
 * Configuration for reading data from AWS S3.
 */
export type S3InputConfig = {
  /**
   * S3 bucket name to access
   */
  bucket_name: string
  consume_strategy?: ConsumeStrategy
  credentials: AwsCredentials
  read_strategy: ReadStrategy
  /**
   * AWS region
   */
  region: string
}
