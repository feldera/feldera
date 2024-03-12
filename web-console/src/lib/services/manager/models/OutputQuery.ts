/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * A query over an output stream.
 *
 * We currently do not support ad hoc queries.  Instead the client can use
 * three pre-defined queries to inspect the contents of a table or view.
 */
export enum OutputQuery {
  TABLE = 'table',
  NEIGHBORHOOD = 'neighborhood',
  QUANTILES = 'quantiles'
}
