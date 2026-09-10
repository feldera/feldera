import { SvelteSet } from 'svelte/reactivity'

// The ids of the metric blocks the user collapsed. Block ids are category ids, so the choice
// follows the user from node to node and outlives a block that leaves and returns.
const collapsed = new SvelteSet<string>()

export const isCollapsed = (id: string): boolean => collapsed.has(id)

export function setCollapsed(id: string, value: boolean): void {
  if (value) {
    collapsed.add(id)
  } else {
    collapsed.delete(id)
  }
}

export const toggleCollapsed = (id: string): void => setCollapsed(id, !collapsed.has(id))
