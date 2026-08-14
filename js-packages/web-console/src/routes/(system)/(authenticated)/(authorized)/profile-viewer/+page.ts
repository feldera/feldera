import type { LoadEvent } from '@sveltejs/kit'

export const prerender = false

export const load = ({ url }: LoadEvent) => {
  return {
    pipelineName: url.searchParams.get('pipelineName') ?? '',
    /**
     * Where the bundle comes from: downloaded from the pipeline ('remote'), or
     * from the user's disk ('upload').
     */
    source: (url.searchParams.get('source') ?? 'remote') as 'remote' | 'upload',
    collect: url.searchParams.get('collect') !== '0',
    /**
     * An uploaded bundle arrives one of two ways. `bundle` names an entry in the
     * bundle history, whose File System Access handle the viewer re-reads from
     * disk — that survives a reload. `channel` is the fallback for browsers
     * without that API: the tab that read the file hands the bytes over once.
     */
    bundle: Number(url.searchParams.get('bundle')) || undefined,
    channel: url.searchParams.get('channel') ?? ''
  }
}
