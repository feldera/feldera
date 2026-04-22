import type { LoadEvent } from '@sveltejs/kit'

export const load = async ({ parent }: LoadEvent) => {
  await parent()
  return {}
}
