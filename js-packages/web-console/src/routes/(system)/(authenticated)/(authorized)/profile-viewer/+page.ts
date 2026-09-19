import type { LoadEvent } from '@sveltejs/kit'

export const prerender = false

export const load = ({ url }: LoadEvent) => {
  return {
    pipelineName: url.searchParams.get('pipelineName') ?? '',
    /**
     * Where the bundle comes from: the running pipeline ('remote'), or the user's own
     * disk ('upload').
     */
    source: (url.searchParams.get('source') ?? 'remote') as 'remote' | 'upload',
    collect: url.searchParams.get('collect') !== '0',
    /**
     * A bundle from the user's disk reaches this page one of two ways. `bundle` names
     * an entry in the bundle history, and the viewer reads the archive itself, so the
     * tab survives a reload. `channel` is the fallback for a bundle that has no
     * history entry: the tab the user picked it in hands the bytes over, once.
     */
    bundle: Number(url.searchParams.get('bundle')) || undefined,
    channel: url.searchParams.get('channel') ?? ''
  }
}
