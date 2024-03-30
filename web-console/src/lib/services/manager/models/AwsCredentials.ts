/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * Configuration to authenticate against AWS
 */
export type AwsCredentials =
  | {
      type: AwsCredentials.type
    }
  | {
      aws_access_key_id: string
      aws_secret_access_key: string
      type: AwsCredentials.type
    }
export namespace AwsCredentials {
  export enum type {
    NO_SIGN_REQUEST = 'NoSignRequest'
  }
}
