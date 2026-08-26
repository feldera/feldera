import type { LoadEvent } from '@sveltejs/kit'

export const prerender = false

export const load = ({ url }: LoadEvent) => {
  return {
    pipelineName: url.searchParams.get('pipelineName') ?? '',
    /** Where the bundle comes from: the pipeline ('remote'), or the user's disk ('upload'). */
    source: (url.searchParams.get('source') ?? 'remote') as 'remote' | 'upload',
    collect: url.searchParams.get('collect') !== '0',
    /**
     * An uploaded bundle arrives one of two ways. `bundle` names an entry in the
     * bundle history, which the viewer reads itself, so the tab survives a reload.
     * `channel` is the fallback for a bundle with no history entry: the tab that read
     * the file hands the bytes over once.
     */
    bundle: Number(url.searchParams.get('bundle')) || undefined,
    channel: url.searchParams.get('channel') ?? ''
  }
}
