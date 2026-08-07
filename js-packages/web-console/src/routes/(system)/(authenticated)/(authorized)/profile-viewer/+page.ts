import type { LoadEvent } from '@sveltejs/kit'

export const prerender = false

export const load = ({ url }: LoadEvent) => {
  return {
    pipelineName: url.searchParams.get('pipelineName') ?? '',
    source: (url.searchParams.get('source') ?? 'remote') as 'remote' | 'upload',
    collect: url.searchParams.get('collect') !== '0',
    channel: url.searchParams.get('channel') ?? ''
  }
}
