import { describe, expect, it } from 'vitest'
import { profileImageFileName } from './fileName'

describe('profileImageFileName', () => {
  it('combines the pipeline name and the snapshot date as YYYY.MM.DD', () => {
    // Month is 0-based: 2 -> March.
    expect(profileImageFileName('my-pipeline', new Date(2024, 2, 9))).toBe(
      'my-pipeline-2024.03.09.svg'
    )
  })

  it('zero-pads the month and day', () => {
    // Without padding this would be "2025.1.5"; the assertion pins the padding.
    expect(profileImageFileName('p', new Date(2025, 0, 5))).toBe('p-2025.01.05.svg')
  })

  it('sanitizes characters that are unsafe in file names', () => {
    expect(profileImageFileName('My Pipeline!/v2', new Date(2024, 2, 9))).toBe(
      'My-Pipeline-v2-2024.03.09.svg'
    )
  })

  it('collapses runs of separators', () => {
    expect(profileImageFileName('a  -  b', new Date(2024, 2, 9))).toBe('a-b-2024.03.09.svg')
  })

  it('falls back to "dataflow-diagram" when the name is missing or empty', () => {
    expect(profileImageFileName(undefined, new Date(2024, 2, 9))).toBe(
      'dataflow-diagram-2024.03.09.svg'
    )
    expect(profileImageFileName('   ', new Date(2024, 2, 9))).toBe('dataflow-diagram-2024.03.09.svg')
  })
})
