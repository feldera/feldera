// What the full layout does to the diagram when a profile loads. The layout mounts the real
// ProfilerDiagram here, unlike in `SupportBundleViewerLayout.svelte.spec.ts`.

import { type CircuitProfile, Visualizer } from 'profiler-lib'
import { TriageResults } from 'triage-types'
import { afterEach, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-svelte'
import { syntheticCircuit } from '../test-support/syntheticCircuit'
import SupportBundleViewerLayout from './SupportBundleViewerLayout.svelte'

afterEach(() => {
  vi.restoreAllMocks()
})

/** Count the passes that color the diagram from the metrics: the one in `Visualizer.render`, and
 *  each later call of `updateMetadata` on the rendering that `render` made. */
const countMetadataPasses = () => {
  const passes = { count: 0 }
  const originalRender = Visualizer.prototype.render
  vi.spyOn(Visualizer.prototype, 'render').mockImplementation(function (
    this: Visualizer,
    circuit: { profile: CircuitProfile; rootNodeId: string }
  ) {
    originalRender.call(this, circuit)
    passes.count++
    // biome-ignore lint/suspicious/noExplicitAny: `rendering` is private to the Visualizer
    const rendering = (this as any).rendering
    const updateMetadata = rendering.updateMetadata.bind(rendering)
    rendering.updateMetadata = (...args: unknown[]) => {
      passes.count++
      return updateMetadata(...args)
    }
  })
  return passes
}

it('colors the diagram once when a profile loads', async () => {
  // The library and the layout must start on the same metric. Else the layout selects its own
  // metric right after the load, and the diagram is colored a second time.
  const passes = countMetadataPasses()
  const circuit = syntheticCircuit({ seed: 1, topLevelItems: 10, workers: 2 })
  const wrapper = document.createElement('div')
  wrapper.style.cssText = 'width:1200px;height:800px'
  document.body.appendChild(wrapper)
  let isLaidOut = false
  const screen = render(SupportBundleViewerLayout, {
    target: wrapper,
    props: {
      profileData: circuit.profile,
      dataflowData: circuit.dataflow,
      triageResults: new TriageResults(),
      profileFiles: [],
      selectedTimestamp: null,
      onSelectTimestamp: () => {},
      sqlPanelFullHeight: false,
      onRenderingChange: (rendering: boolean) => {
        isLaidOut = !rendering
      }
    }
  })
  await expect.poll(() => isLaidOut, { timeout: 30_000 }).toBe(true)
  await new Promise((resolve) => setTimeout(resolve, 100))
  expect(passes.count).toBe(1)
  screen.unmount()
  wrapper.remove()
})
