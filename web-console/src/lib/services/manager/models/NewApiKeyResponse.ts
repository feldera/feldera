/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ApiKeyId } from './ApiKeyId'
/**
 * Response to a successful API key creation.
 */
export type NewApiKeyResponse = {
  /**
   * Generated API key. There is no way to
   * retrieve this key again from the
   * pipeline-manager, so store it securely.
   */
  api_key: string
  api_key_id: ApiKeyId
  /**
   * API key name
   */
  name: string
}
