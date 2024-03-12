/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * Strategy that determines which objects to read from a given bucket
 */
export type ReadStrategy =
  | {
      key: string
      type: ReadStrategy.type
    }
  | {
      prefix: string
      type: ReadStrategy.type
    }
export namespace ReadStrategy {
  export enum type {
    SINGLE_KEY = 'SingleKey'
  }
}
