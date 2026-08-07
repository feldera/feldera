// Component tests for the demo tile's conditional gate: a read-only caller may
// open a demo only when its pipeline already exists; creating one needs
// write:pipeline. Style is disable (the tile stays visible, inert when blocked).

import { afterEach, describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import type { Demo } from '$lib/services/pipelineManager'
import { permissionsOf } from '$lib/services/rbac'

const roleState = vi.hoisted(() => ({ current: 'read' as 'read' | 'write' | 'admin' | 'owner' }))
const listState = vi.hoisted(() => ({ current: [] as { name: string }[] }))

vi.mock('$app/state', () => ({
  page: {
    data: {
      get feldera() {
        return { role: roleState.current, permissions: permissionsOf(roleState.current) }
      }
    }
  }
}))

vi.mock('$lib/compositions/pipelines/usePipelineList.svelte', () => ({
  usePipelineList: () => ({
    get pipelines() {
      return listState.current
    }
  })
}))

const tryPipeline = vi.hoisted(() => vi.fn())

vi.mock('$lib/compositions/pipelines/useTryPipeline', () => ({
  useTryPipeline: () => tryPipeline
}))

import DemoTile from './DemoTile.svelte'

const demo = {
  name: 'demo-1',
  title: 'Demo One',
  description: 'A demo',
  type: 'Tutorial'
} as unknown as Demo

afterEach(() => {
  roleState.current = 'read'
  listState.current = []
  tryPipeline.mockClear()
})

describe('DemoTile.svelte', () => {
  it('enables the tile for a read-only caller when the pipeline already exists', async () => {
    roleState.current = 'read'
    listState.current = [{ name: 'demo-1' }]
    await render(DemoTile, { demo })
    await expect.element(page.getByRole('button', { name: 'Demo One' })).not.toBeDisabled()
  })

  it('disables the tile for a read-only caller when the pipeline does not exist', async () => {
    roleState.current = 'read'
    listState.current = []
    await render(DemoTile, { demo })
    // Reverting the gate leaves the tile enabled for read, failing this.
    await expect.element(page.getByRole('button', { name: 'Demo One' })).toBeDisabled()
  })

  it('enables the tile for a write caller even when the pipeline does not exist', async () => {
    roleState.current = 'write'
    listState.current = []
    await render(DemoTile, { demo })
    await expect.element(page.getByRole('button', { name: 'Demo One' })).not.toBeDisabled()
  })

  it('reports where the demo was opened from', async () => {
    roleState.current = 'write'
    await render(DemoTile, { demo, placement: 'home' })

    await page.getByRole('button', { name: 'Demo One' }).click()

    // The placement reaches the `demo_opened` analytics event through here.
    expect(tryPipeline).toHaveBeenCalledWith(demo, 'home')
  })
})
