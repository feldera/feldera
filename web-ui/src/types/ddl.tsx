// The CSV we get from the ingress rest API doesn't give us a concrete type.
//
// This is problematic because we can't just send all string back for the anchor
// type. They have to represent the original type, a number needs to be a number
// etc. This issue will go away once we support JSON for the data format. But
// until then we do the type conversion here.

import { P, match } from 'ts-pattern'
import { Field } from './manager'

// TODO: Note that all Number in JS are 64-bit floating point numbers. When we
// switch to the JSON format we will just parse everything as BigInt which has
// arbitrary precision so we don't loose anything when displaying numbers from
// dbsp.
export const parseSqlType = (sqlType: Field, value: string) =>
  match(sqlType.columntype)
    .with({ type: 'BOOLEAN' }, () => Boolean(value))
    .with({ type: 'TINYINT' }, () => Number(value))
    .with({ type: 'SMALLINT' }, () => Number(value))
    .with({ type: 'INTEGER' }, () => Number(value))
    .with({ type: 'BIGINT' }, () => Number(value))
    .with({ type: 'DECIMAL' }, () => Number(value))
    .with({ type: 'FLOAT' }, () => Number(value))
    .with({ type: 'DOUBLE' }, () => Number(value))
    .with({ type: 'TIMESTAMP', nullable: P.select() }, (nullable: boolean) => {
      if (!value && nullable) return null
      else return value
    })
    .otherwise(() => value)
