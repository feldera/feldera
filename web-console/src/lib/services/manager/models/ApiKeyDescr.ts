/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ApiKeyId } from './ApiKeyId'
import type { ApiPermission } from './ApiPermission'
/**
 * ApiKey descriptor.
 */
export type ApiKeyDescr = {
  id: ApiKeyId
  name: string
  scopes: Array<ApiPermission>
}
