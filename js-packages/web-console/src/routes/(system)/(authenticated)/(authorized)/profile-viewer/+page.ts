import type { LoadEvent } from '@sveltejs/kit'

export const prerender = false

/**
 * The id of the history entry `bundle` names. `undefined` when the URL carries no
 * `bundle` at all, and `'invalid'` when it carries one that is not a positive integer,
 * which a truncated or hand-edited link looks like. The two cases lead the page down
 * different paths, so a garbled id must not read as an absent one.
 */
const parseBundleId = (raw: string | null): number | 'invalid' | undefined => {
  if (raw === null) {
    return undefined
  }
  const id = Number(raw)
  return Number.isInteger(id) && id > 0 ? id : 'invalid'
}

export const load = ({ url }: LoadEvent) => {
  return {
    pipelineName: url.searchParams.get('pipelineName') ?? '',
    /**
     * Where the bundle comes from: the running pipeline ('remote'), or the user's own
     * disk ('upload'). Anything else the URL says is read as 'remote', so that the page
     * can branch on these two values alone.
     */
    source: url.searchParams.get('source') === 'upload' ? ('upload' as const) : ('remote' as const),
    collect: url.searchParams.get('collect') !== '0',
    /**
     * A bundle from the user's disk reaches this page one of two ways:
     * - `bundle` names an entry in the bundle history, and the viewer reads the archive itself,
     * so the tab survives a reload.
     * - `channel` is the fallback for a bundle that has no history entry:
     * the tab the user picked it in transfers the bytes to the new tab with the profiler layout, once -
     * if the profiler tab is reloaded the same bundle that was loaded before won't reload.
     */
    bundle: parseBundleId(url.searchParams.get('bundle')),
    channel: url.searchParams.get('channel') ?? ''
  }
}
