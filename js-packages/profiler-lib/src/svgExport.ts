// SVG export for the profiler diagram: lazily loads the cytoscape-svg extension, exports the
// whole graph, and frames it with padding and a solid background.

import cytoscape from 'cytoscape';

/** Options for exporting the diagram as SVG. */
export interface SvgExportOptions {
    /** Uniform padding (in diagram units) added around the graph's bounding box. Default 20. */
    padding?: number;
    /** Background color filling the whole image, padding included. Default white ('#ffffff'). */
    background?: string;
}

/** Default padding framed around an exported diagram. */
const DEFAULT_SVG_PADDING = 20;
const DEFAULT_SVG_BACKGROUND = '#ffffff';
const SVG_NAMESPACE = 'http://www.w3.org/2000/svg';

/** Options accepted by the `svg()` method the `cytoscape-svg` extension adds to the core. */
interface CytoscapeSvgOptions {
    /** Export the whole graph rather than only the current viewport. */
    full?: boolean;
    /** Scale factor applied to the exported drawing. */
    scale?: number;
    /** Background color painted behind the graph (transparent when omitted). */
    bg?: string;
}

/** The cytoscape core once the `cytoscape-svg` extension is registered. */
interface SvgCapableCore {
    svg(options?: CytoscapeSvgOptions): string;
}

// The extension registers a global `svg` core method; register it once per page. Loaded lazily
// because its UMD bundle touches `window` at import time, which would break Node environments
// (e.g. unit tests) that never render.
let svgExtensionRegistered = false;
async function ensureSvgExtension(): Promise<void> {
    if (svgExtensionRegistered) {
        return;
    }
    const { default: cytoscapeSvg } = await import('cytoscape-svg');
    cytoscape.use(cytoscapeSvg);
    svgExtensionRegistered = true;
}

/**
 * Export a cytoscape graph to a standalone SVG document string. Exports the whole graph (not just
 * the viewport), framed with padding and a solid background.
 */
export async function exportGraphSvg(
    cy: cytoscape.Core,
    options?: SvgExportOptions
): Promise<string> {
    await ensureSvgExtension();
    const padding = options?.padding ?? DEFAULT_SVG_PADDING;
    const background = options?.background ?? DEFAULT_SVG_BACKGROUND;
    // Export the tight bounding box transparently, then frame it so the background covers the
    // padding too (cytoscape-svg fills its background only behind the content box).
    const svg = (cy as unknown as SvgCapableCore).svg({ full: true });
    return frameSvg(svg, padding, background);
}

/**
 * Frame a `cytoscape-svg` document: add uniform padding around the graph and a solid background
 * that also covers the padding. `cytoscape-svg` exports a tight bounding box and paints its
 * background only behind the content, so padding and background are added here instead.
 *
 * Exported for testing. Returns the input unchanged when it cannot be parsed or has no size.
 */
export function frameSvg(svg: string, padding: number, background: string): string {
    const doc = new DOMParser().parseFromString(svg, 'image/svg+xml');
    const root = doc.documentElement;
    if (root.localName !== 'svg' || root.getElementsByTagName('parsererror').length > 0) {
        return svg;
    }
    const width = Number.parseFloat(root.getAttribute('width') ?? '');
    const height = Number.parseFloat(root.getAttribute('height') ?? '');
    if (!Number.isFinite(width) || !Number.isFinite(height)) {
        return svg;
    }

    const pad = Math.max(0, padding);
    const framedWidth = width + 2 * pad;
    const framedHeight = height + 2 * pad;

    // Move the existing content into a group offset by the padding.
    const content = doc.createElementNS(SVG_NAMESPACE, 'g');
    content.setAttribute('transform', `translate(${pad},${pad})`);
    while (root.firstChild) {
        content.appendChild(root.firstChild);
    }

    // Background rect first (behind the content), sized to the full framed canvas.
    const backgroundRect = doc.createElementNS(SVG_NAMESPACE, 'rect');
    backgroundRect.setAttribute('x', '0');
    backgroundRect.setAttribute('y', '0');
    backgroundRect.setAttribute('width', String(framedWidth));
    backgroundRect.setAttribute('height', String(framedHeight));
    backgroundRect.setAttribute('fill', background);
    root.appendChild(backgroundRect);
    root.appendChild(content);

    root.setAttribute('width', String(framedWidth));
    root.setAttribute('height', String(framedHeight));
    root.setAttribute('viewBox', `0 0 ${framedWidth} ${framedHeight}`);

    return new XMLSerializer().serializeToString(root);
}
