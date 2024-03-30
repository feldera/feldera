/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { JsonFlavor } from './JsonFlavor'
import type { JsonUpdateFormat } from './JsonUpdateFormat'
/**
 * JSON parser configuration.
 *
 * Describes the shape of an input JSON stream.
 *
 * # Examples
 *
 * A configuration with `update_format="raw"` and `array=false`
 * is used to parse a stream of JSON objects without any envelope
 * that get inserted in the input table.
 *
 * ```json
 * {"b": false, "i": 100, "s": "foo"}
 * {"b": true, "i": 5, "s": "bar"}
 * ```
 *
 * A configuration with `update_format="insert_delete"` and
 * `array=false` is used to parse a stream of JSON data change events
 * in the insert/delete format:
 *
 * ```json
 * {"delete": {"b": false, "i": 15, "s": ""}}
 * {"insert": {"b": false, "i": 100, "s": "foo"}}
 * ```
 *
 * A configuration with `update_format="insert_delete"` and
 * `array=true` is used to parse a stream of JSON arrays
 * where each array contains multiple data change events in
 * the insert/delete format.
 *
 * ```json
 * [{"insert": {"b": true, "i": 0}}, {"delete": {"b": false, "i": 100, "s": "foo"}}]
 * ```
 */
export type JsonParserConfig = {
  /**
   * Set to `true` if updates in this stream are packaged into JSON arrays.
   *
   * # Example
   *
   * ```json
   * [{"b": true, "i": 0},{"b": false, "i": 100, "s": "foo"}]
   * ```
   */
  array?: boolean
  json_flavor?: JsonFlavor
  update_format?: JsonUpdateFormat
}
