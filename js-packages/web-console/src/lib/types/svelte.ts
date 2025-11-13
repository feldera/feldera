import type { Snippet as SvelteSnippet } from 'svelte'

export type Snippet<T extends any[] = []> = (...params: T) => ReturnType<SvelteSnippet<T>>
