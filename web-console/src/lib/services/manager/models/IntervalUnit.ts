/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * The specified units for SQL Interval types.
 *
 * `INTERVAL 1 DAY`, `INTERVAL 1 DAY TO HOUR`, `INTERVAL 1 DAY TO MINUTE`,
 * would yield `Day`, `DayToHour`, `DayToMinute`, as the `IntervalUnit` respectively.
 */
export enum IntervalUnit {
  DAY = 'DAY',
  DAYTOHOUR = 'DAYTOHOUR',
  DAYTOMINUTE = 'DAYTOMINUTE',
  DAYTOSECOND = 'DAYTOSECOND',
  HOUR = 'HOUR',
  HOURTOMINUTE = 'HOURTOMINUTE',
  HOURTOSECOND = 'HOURTOSECOND',
  MINUTE = 'MINUTE',
  MINUTETOSECOND = 'MINUTETOSECOND',
  MONTH = 'MONTH',
  SECOND = 'SECOND',
  YEAR = 'YEAR',
  YEARTOMONTH = 'YEARTOMONTH'
}
