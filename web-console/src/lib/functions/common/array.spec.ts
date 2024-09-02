import { groupBy } from '$lib/functions/common/array'
import { expect, test } from '@playwright/experimental-ct-svelte'

test('groupBy', async () => {
  expect(
    groupBy(
      [
        { key: 'a', value: 2 },
        { key: 'a', value: 1 },
        { key: 'b', value: 1 },
        { key: 'b', value: 3 },
        { key: 'b', value: 2 },
        { key: 'a', value: 3 }
      ],
      (v) => v.key
    )
  ).toEqual([
    [
      'a',
      [
        { key: 'a', value: 2 },
        { key: 'a', value: 1 },
        { key: 'a', value: 3 }
      ]
    ],
    [
      'b',
      [
        { key: 'b', value: 1 },
        { key: 'b', value: 3 },
        { key: 'b', value: 2 }
      ]
    ]
  ])
})
