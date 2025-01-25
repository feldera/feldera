import { Relation } from '$lib/services/manager'

import { expect, test } from '@playwright/experimental-ct-react'

import { JSONXgressValue, SQLValueJS, xgressJSONToSQLRecord } from './sqlValue'

test('Test parsing of column names of JSON xgress payload in insert/delete format', async () => {
  const cases: [Relation, Record<string, JSONXgressValue>, Record<string, SQLValueJS> | 'error'][] =
    [
      [
        {
          name: 'average_price',
          case_sensitive: false,
          fields: [
            {
              name: 'EXPR$0',
              case_sensitive: false,
              columntype: {
                type: 'DOUBLE',
                nullable: true
              }
            }
          ]
        },
        {
          expr$0: null
        },
        {
          expr$0: null
        }
      ],
      [
        {
          name: 'test',
          case_sensitive: false,
          fields: [
            {
              name: 'fielda',
              case_sensitive: false,
              columntype: {
                type: 'INTEGER',
                nullable: true
              }
            },
            {
              name: 'FieldB',
              case_sensitive: false,
              columntype: {
                type: 'INTEGER',
                nullable: true
              }
            },
            {
              name: 'FieldC',
              case_sensitive: true,
              columntype: {
                type: 'INTEGER',
                nullable: true
              }
            }
          ]
        },
        {
          fielda: null,
          fieldb: null,
          '"FieldC"': null
        },
        {
          fielda: null,
          fieldb: null,
          '"FieldC"': null
        }
      ],
      [
        {
          name: 'test',
          case_sensitive: false,
          fields: [
            {
              name: 'FieldB',
              case_sensitive: false,
              columntype: {
                type: 'INTEGER',
                nullable: true
              }
            }
          ]
        },
        {
          FieldB: null
        },
        'error'
      ]
    ]
  for (const [relation, value, record] of cases) {
    expect(
      (() => {
        try {
          return xgressJSONToSQLRecord(relation, value)
        } catch {
          return 'error'
        }
      })()
    ).toEqual(record)
  }
})
