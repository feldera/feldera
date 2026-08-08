// @vitest-environment happy-dom
// A DOM environment is required only so the static cytoscape / cytoscape-dblclick imports (which
// reference `window` at load time) resolve; this test itself renders nothing.
import { describe, expect, it } from 'vitest'
import { frameSvg } from './svgExport.js'
import { type ProfilerCallbacks, Visualizer, type VisualizerConfig } from './profiler.js'

const noopCallbacks: ProfilerCallbacks = {
    displayNodeAttributes: () => {},
    displayTopNodes: () => {},
    onMetricsChanged: () => {},
    displayMessage: () => {},
    onError: () => {}
}

describe('Visualizer.exportSvg', () => {
    it('returns null before any profile is rendered', async () => {
        // This environment has no DOM. The constructor only stores its config (it never touches
        // the containers), so dummy elements are enough to exercise the pre-render guard. Dropping
        // that guard would dereference a null `rendering` here and throw instead of returning null.
        const config = {
            graphContainer: {} as HTMLElement,
            navigatorContainer: {} as HTMLElement,
            callbacks: noopCallbacks
        } satisfies VisualizerConfig
        const visualizer = new Visualizer(config)
        expect(await visualizer.exportSvg()).toBeNull()
    })
})

describe('frameSvg', () => {
    const svg = (attrs: string, body = '<circle cx="10" cy="10" r="5"/>') =>
        `<svg xmlns="http://www.w3.org/2000/svg" ${attrs}>${body}</svg>`

    it('grows the canvas by padding on every edge and adds a matching viewBox', () => {
        const framed = frameSvg(svg('width="100" height="60"'), 20, '#ffffff')
        const root = new DOMParser().parseFromString(framed, 'image/svg+xml').documentElement
        // 100 + 2*20 wide, 60 + 2*20 tall.
        expect(root.getAttribute('width')).toBe('140')
        expect(root.getAttribute('height')).toBe('100')
        expect(root.getAttribute('viewBox')).toBe('0 0 140 100')
    })

    it('offsets the original content by the padding and keeps it', () => {
        const framed = frameSvg(svg('width="100" height="60"'), 20, '#ffffff')
        const root = new DOMParser().parseFromString(framed, 'image/svg+xml').documentElement
        const group = root.querySelector('g[transform]')
        expect(group?.getAttribute('transform')).toBe('translate(20,20)')
        expect(group?.querySelector('circle')).not.toBeNull()
    })

    it('paints a background rect that covers the whole framed canvas', () => {
        const framed = frameSvg(svg('width="100" height="60"'), 20, '#123456')
        const root = new DOMParser().parseFromString(framed, 'image/svg+xml').documentElement
        const rect = root.querySelector('rect')
        expect(rect?.getAttribute('fill')).toBe('#123456')
        expect(rect?.getAttribute('width')).toBe('140')
        expect(rect?.getAttribute('height')).toBe('100')
    })

    it('returns the input unchanged when the size is unknown', () => {
        // No width/height to frame around; framing would produce a zero-size canvas, so leave it.
        const input = svg('', '<g/>')
        expect(frameSvg(input, 20, '#ffffff')).toBe(input)
    })
})
