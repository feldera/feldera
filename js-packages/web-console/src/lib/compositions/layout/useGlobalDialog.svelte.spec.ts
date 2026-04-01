import { describe, expect, it } from 'vitest'
import { useGlobalDialog } from './useGlobalDialog.svelte'

describe('useGlobalDialog', () => {
  it('initially has null dialog and null onClickAway', () => {
    const gd = useGlobalDialog()
    expect(gd.dialog).toBeNull()
    expect(gd.onClickAway).toBeNull()
  })

  it('setting dialog updates the value', () => {
    const gd = useGlobalDialog()
    const snippet = (() => {}) as any
    gd.dialog = snippet
    expect(gd.dialog).toBe(snippet)
  })

  it('clearing dialog also clears onClickAway', () => {
    const gd = useGlobalDialog()
    gd.dialog = (() => {}) as any
    gd.onClickAway = () => {}
    expect(gd.onClickAway).not.toBeNull()

    gd.dialog = null
    expect(gd.dialog).toBeNull()
    expect(gd.onClickAway).toBeNull()
  })

  it('setting dialog to a new value does not clear onClickAway', () => {
    const gd = useGlobalDialog()
    const handler = () => {}
    gd.dialog = (() => {}) as any
    gd.onClickAway = handler

    gd.dialog = (() => {}) as any
    expect(gd.onClickAway).toBe(handler)
  })

  it('returns the same shared state across multiple calls', () => {
    const gd1 = useGlobalDialog()
    const gd2 = useGlobalDialog()
    const snippet = (() => {}) as any
    gd1.dialog = snippet
    expect(gd2.dialog).toBe(snippet)
  })
})
