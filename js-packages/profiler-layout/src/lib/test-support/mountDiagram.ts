// Harness for the pixel-level diagram tests. profiler-lib paints the selection glow and a node's text
// straight onto the cytoscape canvas, through renderer hooks that only exist in a browser, and its
// own suite is headless - so the claims that can only be made about pixels are made here, over a
// really mounted diagram.

import type { NodeAttributes, Option, ProfilerCallbacks } from 'profiler-lib'
import { render } from 'vitest-browser-svelte'
import ThemedDiagram from './ThemedDiagram.svelte'

const simple = (id: string, label: string) => ({ Simple: { id, label } })
const cluster = (id: string, label: string, nodes: unknown[]) => ({ Cluster: { id, label, nodes } })

/** Two operators, no edges and no source positions, so the space around each node is empty and a
 *  sampled pixel there can only be the node's own glow. */
export const OPERATORS = {
  metrics: [],
  worker_profiles: [{ metadata: {} }],
  graph: {
    nodes: { id: 'n', label: 'circuit', nodes: [simple('n1', 'first'), simple('n2', 'second')] },
    edges: []
  }
}

/** One operator inside a region, which a circuit this small renders expanded - `collapse()` is what
 *  turns the region into a collapsed composite. */
export const COMPOSITE = {
  metrics: [],
  worker_profiles: [{ metadata: {} }],
  graph: {
    nodes: {
      id: 'n',
      label: 'circuit',
      nodes: [cluster('region', 'shard', [simple('n1', 'MapIndexedZSet')])]
    },
    edges: []
  }
}

/** One short operator inside a region with a long name. A region is sized by the nodes it holds, so
 *  nothing but its own name can make this one wide - which is what the region has to leave room for. */
export const WIDE_REGION = {
  metrics: [],
  worker_profiles: [{ metadata: {} }],
  graph: {
    nodes: {
      id: 'n',
      label: 'circuit',
      nodes: [cluster('region', 'shard_by_index_and_hash_of_key', [simple('n1', 'x')])]
    },
    edges: []
  }
}

/** Two operators with SQL source attached, which is what puts a code chip on each: one inside a region,
 *  where a press on the chip could fall through to the region behind it, and one on the graph itself,
 *  where the same press could fall through to the background. Source positions come from a dataflow graph
 *  matched to the profile by persistent id, not from the profile, so this fixture is the two of them
 *  together. The edges are there for what a click must not color. */
export const WITH_SOURCE = {
  profile: {
    metrics: [],
    worker_profiles: [
      {
        metadata: {
          n0: [
            { metric_id: 'persistent_id', value: { type: 'string', value: 'op-0' } },
            { metric_id: 'used_bytes', value: { type: 'bytes', value: 1024 } }
          ],
          n1: [
            { metric_id: 'persistent_id', value: { type: 'string', value: 'op-1' } },
            { metric_id: 'used_bytes', value: { type: 'bytes', value: 4096 } }
          ],
          n2: [{ metric_id: 'used_bytes', value: { type: 'bytes', value: 2048 } }]
        }
      }
    ],
    graph: {
      nodes: {
        id: 'n',
        label: 'circuit',
        nodes: [
          simple('n0', 'source'),
          cluster('region', 'shard', [simple('n1', 'map'), simple('n2', 'filter')])
        ]
      },
      edges: [
        { from_node: 'n0', to_node: 'n1' },
        { from_node: 'n1', to_node: 'n2' }
      ]
    }
  },
  dataflow: {
    calcite_plan: {},
    sources: ['CREATE TABLE fact_1 (id BIGINT);'],
    mir: {
      m0: {
        operation: 'source',
        table: null,
        view: null,
        inputs: [],
        calcite: {},
        persistent_id: 'op-0',
        positions: [
          { start_line_number: 1, start_column: 1, end_line_number: 1, end_column: 12 }
        ]
      },
      m1: {
        operation: 'map',
        table: null,
        view: null,
        inputs: [],
        calcite: {},
        persistent_id: 'op-1',
        positions: [
          { start_line_number: 1, start_column: 14, end_line_number: 1, end_column: 19 }
        ]
      }
    }
  }
}

/** A long chain of operators: enough of them that the whole graph does not fit on screen, which is
 *  what makes the initial zoom and the zoom a search moves to observable - both are clamped to
 *  whatever fitting the graph takes, and a two-node graph fits at any zoom. */
export const MANY = {
  metrics: [],
  worker_profiles: [{ metadata: {} }],
  graph: {
    nodes: {
      id: 'n',
      label: 'circuit',
      nodes: Array.from({ length: 60 }, (_, i) => simple(`n${i}`, `operator ${i}`))
    },
    edges: Array.from({ length: 59 }, (_, i) => ({ from_node: `n${i}`, to_node: `n${i + 1}` }))
  }
}

export interface Rgba {
  r: number
  g: number
  b: number
  a: number
}

/** How far apart two colors are, summed over the channels: how much a pixel stands out from the
 *  surface behind it. */
export const colorDistance = (a: Rgba, b: Rgba): number =>
  Math.abs(a.r - b.r) + Math.abs(a.g - b.g) + Math.abs(a.b - b.b)

export const settle = () => new Promise((resolve) => setTimeout(resolve, 150))

/** Mount the diagram over `profile`, wait for its layout, and hand back the cytoscape instance with
 *  a pixel probe over its canvases. Zoomed in afterwards, since both the glow and a 12px label are a
 *  handful of device pixels at the zoom a profile opens at - pass `keepOpeningView` to leave the view
 *  exactly as the diagram placed it, which is what the view's own tests are about. */
export async function mountDiagram(
  theme: 'light' | 'dark',
  profile: unknown = OPERATORS,
  keepOpeningView = false,
  dataflow?: unknown
) {
  document.documentElement.dataset.theme = theme
  const wrapper = document.createElement('div')
  wrapper.style.cssText = 'position:relative;width:900px;height:600px'
  document.body.appendChild(wrapper)

  let laidOut: () => void
  let layout = new Promise<void>((resolve) => (laidOut = resolve))
  /** What the diagram reported through its callbacks, in the order it reported it. A click is only
   *  half observable from the canvas - what reaches the application is the other half. */
  const reported = {
    nodeClicks: [] as string[],
    doubleClicks: [] as Array<{ nodeId: string, type: 'group' | 'leaf' }>,
    /** One entry per `displayNodeAttributes`, the node it carried or `null` for a dismissal. */
    attributes: [] as Array<{ nodeId: string | null, isSticky: boolean }>
  }
  const callbacks = {
    displayNodeAttributes: (data: Option<NodeAttributes>, isSticky: boolean) => {
      reported.attributes.push({
        nodeId: data.match({ some: (v) => v.nodeId, none: () => null }),
        isSticky
      })
    },
    displayTopNodes: () => {},
    onMetricsChanged: () => {},
    onWorkersChanged: () => {},
    displayMessage: () => {},
    onNodeClick: (nodeId: string) => reported.nodeClicks.push(nodeId),
    onNodeDoubleClick: (nodeId: string, type: 'group' | 'leaf') => {
      reported.doubleClicks.push({ nodeId, type })
    },
    onError: (e: string) => {
      throw new Error(e)
    },
    onRenderingChange: (inFlight: boolean) => {
      if (!inFlight) {
        laidOut()
      }
    }
  } as unknown as ProfilerCallbacks

  const props = {
    profileData: profile as never,
    dataflowData: dataflow as never,
    programCode: undefined,
    callbacks,
    theme
  }
  const rendered = render(ThemedDiagram, { target: wrapper, props })
  const component = rendered.component as unknown as {
    search(query: string): void
    setTheme(theme: 'light' | 'dark'): void
    showGlobalMetrics(isSticky?: boolean): void
  }
  await layout
  await settle()

  const container = wrapper.querySelector('.visualizer-graph') as HTMLElement & {
    // biome-ignore lint/suspicious/noExplicitAny: the registry cytoscape leaves on its container
    _cyreg: { cy: any }
  }
  const cy = container._cyreg.cy
  if (!keepOpeningView) {
    cy.maxZoom(8)
    cy.zoom(2)
  }

  const dpr = window.devicePixelRatio
  const canvases = Array.from(container.querySelectorAll('canvas'))
  /** The pixel at a rendered position, taken from the topmost canvas that painted there. */
  const pixelAt = (x: number, y: number): Rgba => {
    for (const canvas of canvases) {
      const data = canvas
        .getContext('2d')!
        .getImageData(Math.round(x * dpr), Math.round(y * dpr), 1, 1).data
      if (data[3] !== 0) {
        return { r: data[0]!, g: data[1]!, b: data[2]!, a: data[3]! / 255 }
      }
    }
    return { r: 0, g: 0, b: 0, a: 0 }
  }

  /** The pixel `distance` outside the node's own box, in the given direction. */
  const outside = (
    id: string,
    direction: 'below' | 'above' | 'right' | 'left',
    distance: number
  ): Rgba => {
    const node = cy.$id(id)
    const { x, y } = node.renderedPosition()
    const halfW = node.renderedOuterWidth() / 2
    const halfH = node.renderedOuterHeight() / 2
    switch (direction) {
      case 'below':
        return pixelAt(x, y + halfH + distance)
      case 'above':
        return pixelAt(x, y - halfH - distance)
      case 'right':
        return pixelAt(x + halfW + distance, y)
      case 'left':
        return pixelAt(x - halfW - distance, y)
    }
  }

  /** Every column of a horizontal band of the node, given as the pixel in it that stands out most
   *  from `surface` - the ink of whatever text is drawn there. `band` is the fraction of the node's
   *  height the strip is centered on, `0` being its top edge and `1` its bottom, and `bandHeight` is
   *  how much of the height it spans. `inset`, in graph pixels, is how much of either side to leave
   *  out; it defaults to the node's padding, which is where its border is drawn - pass a smaller one to
   *  reach the corner chips, which sit inside that padding.
   */
  const inkColumns = (id: string, band: number, surface: Rgba, bandHeight = 0.25, inset?: number) => {
    const node = cy.$id(id)
    const { x, y } = node.renderedPosition()
    const padding = (inset ?? Number(node.numericStyle('padding'))) * cy.zoom()
    const halfW = node.renderedOuterWidth() / 2 - padding
    const height = node.renderedOuterHeight()
    const top = y - height / 2 + band * height
    const columns: Array<{ x: number, distance: number, color: Rgba }> = []
    for (let px = Math.ceil(x - halfW); px <= Math.floor(x + halfW); px++) {
      let best = { x: px, distance: -1, color: surface }
      const reach = (height * bandHeight) / 2
      for (let py = Math.round(top - reach); py <= Math.round(top + reach); py++) {
        const color = pixelAt(px, py)
        const distance = colorDistance(color, surface)
        if (distance > best.distance) {
          best = { x: px, distance, color }
        }
      }
      columns.push(best)
    }
    return columns
  }

  /** The node's own fill, sampled below its text row. */
  const nodeFill = (id: string): Rgba => {
    const node = cy.$id(id)
    const { x, y } = node.renderedPosition()
    return pixelAt(x, y + node.renderedOuterHeight() * 0.35)
  }

  /** The minimap: its frame, the canvas holding the picture of the circuit, the outline of the viewport
   *  over it, and readers for the pixels of the picture. Every point below is in the map's own CSS
   *  pixels; the picture is painted in device pixels and scaled on the way in. */
  const minimap = () => {
    const root = wrapper.querySelector('#navigator') as HTMLDivElement
    const canvas = root.querySelector('canvas') as HTMLCanvasElement
    const context = canvas.getContext('2d')!
    // The canvas is the map. Measured from its rect, since `clientWidth` rounds off the fractional size
    // the circuit's shape gives it.
    const rect = canvas.getBoundingClientRect()
    // Per axis: the canvas holds a whole number of device pixels either way, so a map a fraction of a
    // pixel wide is scaled by a hair more in x than in y.
    const scale = { x: canvas.width / rect.width, y: canvas.height / rect.height }
    return {
      root,
      canvas,
      view: root.querySelector('#navigator-viewport') as HTMLDivElement,
      /** Size of the map, in its own pixels. */
      size: { w: rect.width, h: rect.height },
      /** The pixel at a point on the map. */
      ink: (x: number, y: number): Rgba => {
        const data = context.getImageData(Math.round(x * scale.x), Math.round(y * scale.y), 1, 1).data
        return { r: data[0]!, g: data[1]!, b: data[2]!, a: data[3]! / 255 }
      },
      /** How many pixels of the picture are painted in `color`, counting only the ones solid enough to
       *  see. A canvas keeps its colors multiplied by the alpha, so a faint pixel comes back off its own
       *  color and towards black - and a mark half a pixel wide would otherwise be read as any dark
       *  color asked for. */
      painted: (color: Rgba, tolerance = 12, minAlpha = 0.5): number => {
        const { data } = context.getImageData(0, 0, canvas.width, canvas.height)
        let count = 0
        for (let i = 0; i < data.length; i += 4) {
          const pixel = { r: data[i]!, g: data[i + 1]!, b: data[i + 2]!, a: data[i + 3]! / 255 }
          if (pixel.a >= minAlpha && colorDistance(pixel, color) <= tolerance) {
            count++
          }
        }
        return count
      },
      /** How much of the picture is painted: the count, and the box it covers. */
      inked: () => {
        const { data } = context.getImageData(0, 0, canvas.width, canvas.height)
        let count = 0
        const box = { x1: Infinity, y1: Infinity, x2: -Infinity, y2: -Infinity }
        for (let i = 3; i < data.length; i += 4) {
          if (data[i]! === 0) {
            continue
          }
          count++
          const pixel = (i - 3) / 4
          const x = (pixel % canvas.width) / scale.x
          const y = Math.floor(pixel / canvas.width) / scale.y
          box.x1 = Math.min(box.x1, x)
          box.y1 = Math.min(box.y1, y)
          box.x2 = Math.max(box.x2, x)
          box.y2 = Math.max(box.y2, y)
        }
        return { count, ...box }
      },
      /** Where a model point lands on the map. */
      at: (model: { x: number, y: number }) => {
        const graph = cy.elements().boundingBox()
        const mapped = rect.width / graph.w
        return { x: (model.x - graph.x1) * mapped, y: (model.y - graph.y1) * mapped }
      },
      /** Press the map at a point of its own, then drag to each point after that. A pointer event
       *  carries whole pixels, so a point is aimed at to the pixel and no closer. */
      drag: async (...points: Array<{ x: number, y: number }>) => {
        const event = (type: string, point: { x: number, y: number }, target: EventTarget) =>
          target.dispatchEvent(
            new PointerEvent(type, {
              clientX: Math.round(rect.left + point.x),
              clientY: Math.round(rect.top + point.y),
              bubbles: true,
              button: 0,
              buttons: 1
            })
          )
        const [first, ...rest] = points
        event('pointerdown', first!, root)
        for (const point of rest) {
          event('pointermove', point, window)
        }
        event('pointerup', points[points.length - 1]!, window)
        await settle()
      }
    }
  }

  /** Switch the palette, the way the application does. The diagram restyles what is on screen, so
   *  nothing moves. */
  const setTheme = async (next: 'light' | 'dark') => {
    document.documentElement.dataset.theme = next
    component.setTheme(next)
    await settle()
  }

  /** Expand or collapse a composite, the way a double click on it does. */
  const toggle = async (id: string) => {
    cy.$id(id).emit('dblclick')
    await settle()
    await settle()
  }

  /** Dispatch a real mouse event at a rendered position, so that cytoscape resolves the position and
   *  the target itself. Dispatched on the canvas the pointer would really be over and bubbled from
   *  there: cytoscape listens on its container and on the window, but ignores any event whose target
   *  is not inside that container - a mousemove on the window alone is dropped. */
  const pointer = (type: 'mousemove' | 'mousedown' | 'mouseup', x: number, y: number) => {
    const rect = container.getBoundingClientRect()
    const surface = container.querySelector('canvas') ?? container
    surface.dispatchEvent(
      new MouseEvent(type, {
        clientX: rect.left + x,
        clientY: rect.top + y,
        bubbles: true,
        button: 0,
        buttons: type === 'mousemove' ? 0 : 1
      })
    )
  }

  /** Move onto a rendered position and press it, the way a mouse click does. */
  const press = async (x: number, y: number) => {
    pointer('mousemove', x, y)
    pointer('mousedown', x, y)
    pointer('mouseup', x, y)
    await settle()
  }

  return {
    cy,
    container,
    reported,
    pixelAt,
    outside,
    inkColumns,
    nodeFill,
    toggle,
    setTheme,
    minimap,
    pointer,
    press,
    diagram: component,
    cleanup: () => wrapper.remove()
  }
}
