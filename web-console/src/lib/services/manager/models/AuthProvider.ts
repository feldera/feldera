/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ProviderAwsCognito } from './ProviderAwsCognito'
import type { ProviderGoogleIdentity } from './ProviderGoogleIdentity'
export type AuthProvider =
  | {
      AwsCognito: ProviderAwsCognito
    }
  | {
      GoogleIdentity: ProviderGoogleIdentity
    }
