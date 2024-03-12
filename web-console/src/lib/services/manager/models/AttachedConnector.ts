/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * Format to add attached connectors during a config update.
 */
export type AttachedConnector = {
  /**
   * The name of the connector to attach.
   */
  connector_name: string
  /**
   * True for input connectors, false for output connectors.
   */
  is_input: boolean
  /**
   * A unique identifier for this attachement.
   */
  name: string
  /**
   * The table or view this connector is attached to. Unquoted
   * table/view names in the SQL program need to be capitalized
   * here. Quoted table/view names have to exactly match the
   * casing from the SQL program.
   */
  relation_name: string
}
