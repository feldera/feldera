/**
 * Gating tests for the right-hand Inspect panel. The Ad-Hoc Queries tab reads
 * live pipeline data and is hidden without `exec:pipeline_data`; the Samply
 * profiling tab stays. A saved selection pointing at the hidden Ad-Hoc tab is
 * dropped to the first visible tab on init.
 *
 * Only the pipeline-manager network surface is mocked. A `Stopped` pipeline
 * makes neither panel fetch on mount, so the render exercises the real tab
 * wiring without any live requests.
 */
import { afterEach, describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-svelte'
import { permissionsOf } from '$lib/services/rbac'

const roleState = vi.hoisted(() => ({ current: 'write' as 'read' | 'write' | 'admin' | 'owner' }))
vi.mock('$app/state', () => ({
  page: {
    data: {
      get feldera() {
        return { role: roleState.current, permissions: permissionsOf(roleState.current) }
      }
    }
  }
}))

vi.mock('$lib/compositions/usePipelineManager.svelte', () => ({
  usePipelineManager: () => ({ adHocQuery: vi.fn() })
}))

// Imported AFTER vi.mock so the mocks take effect.
import InteractionPanel from './InteractionPanel.svelte'

let testCounter = 0
const nextPipelineName = () => `interaction-gating-${++testCounter}`

let mounted: { unmount: () => Promise<void> } | undefined
let mountTarget: HTMLDivElement | undefined

function mountPanel(opts: { role: 'read' | 'write'; seedTab?: string }) {
  roleState.current = opts.role
  const name = nextPipelineName()
  if (opts.seedTab) {
    localStorage.setItem(`pipelines/${name}/currentInteractionTab`, JSON.stringify(opts.seedTab))
  }

  mountTarget = document.createElement('div')
  mountTarget.style.cssText = 'height: 600px; width: 800px; display: flex; flex-direction: column;'
  document.body.appendChild(mountTarget)

  mounted = render(InteractionPanel, {
    target: mountTarget,
    props: {
      pipeline: { current: { name, status: 'Stopped' } },
      metrics: { current: {} },
      deleted: false,
      currentTab: null
    }
  } as any)
}

const tabTexts = () =>
  Array.from(document.querySelectorAll('[role="tab"]')).map((t) => t.textContent ?? '')

describe('InteractionPanel — exec:pipeline_data tab gating', () => {
  afterEach(async () => {
    await mounted?.unmount()
    mounted = undefined
    mountTarget?.remove()
    mountTarget = undefined
    localStorage.clear()
    roleState.current = 'write'
    vi.clearAllMocks()
  })

  it('shows the Ad-Hoc Queries tab for a write caller', async () => {
    mountPanel({ role: 'write' })
    await expect.poll(() => tabTexts().length).toBeGreaterThan(0)
    expect(tabTexts().some((t) => t.includes('Ad-Hoc'))).toBe(true)
  })

  it('hides the Ad-Hoc Queries tab for a read-only caller', async () => {
    mountPanel({ role: 'read' })
    await expect.poll(() => tabTexts().length).toBeGreaterThan(0)
    // Reverting the gate (dropping the exec:pipeline_data filter) renders the
    // Ad-Hoc trigger and fails this test.
    expect(tabTexts().some((t) => t.includes('Ad-Hoc'))).toBe(false)
    expect(tabTexts().some((t) => t.includes('CPU Profile'))).toBe(true)
  })

  it('drops a saved Ad-Hoc selection to the first visible tab for a read caller', async () => {
    mountPanel({ role: 'read', seedTab: 'Ad-Hoc Queries' })
    await expect.poll(() => tabTexts().length).toBeGreaterThan(0)
    // Only Samply remains, and it is the active tab (a blank panel would mean the
    // saved Ad-Hoc selection survived the reset).
    expect(tabTexts().some((t) => t.includes('Ad-Hoc'))).toBe(false)
    expect(tabTexts().some((t) => t.includes('CPU Profile'))).toBe(true)
  })
})
