<script lang="ts">
  import { untrack } from 'svelte'
  import PipelineStatusDot from '$lib/components/pipelines/list/PipelineStatusDot.svelte'
  import { resolve } from '$lib/functions/svelte'
  import { type PipelineThumb } from '$lib/services/pipelineManager'

  let {
    pipelines,
    pipelineName,
    onaction
  }: {
    pipelines: PipelineThumb[]
    pipelineName: string
    onaction?: () => void
  } = $props()

  type FolderNode = { kind: 'folder'; path: string; name: string; children: TreeNode[] }
  type TreeNode = FolderNode | { kind: 'leaf'; item: PipelineThumb }

  const getFolderPath = (p: PipelineThumb): string => {
    return (p.path ?? '').replace(/^\/+|\/+$/g, '')
  }

  const sortNode = (node: FolderNode) => {
    node.children.sort((a, b) => {
      if (a.kind !== b.kind) {
        return a.kind === 'folder' ? -1 : 1
      }
      const aName = a.kind === 'folder' ? a.name : a.item.name
      const bName = b.kind === 'folder' ? b.name : b.item.name
      return aName.localeCompare(bName)
    })
    for (const child of node.children) {
      if (child.kind === 'folder') {
        sortNode(child)
      }
    }
  }

  const tree = $derived.by(() => {
    const root: FolderNode = { kind: 'folder', path: '', name: '', children: [] }
    const folderByPath = new Map<string, FolderNode>()
    folderByPath.set('', root)
    const ensureFolder = (path: string): FolderNode => {
      let f = folderByPath.get(path)
      if (f) {
        return f
      }
      const parts = path.split('/').filter(Boolean)
      const parentPath = parts.slice(0, -1).join('/')
      const parent = ensureFolder(parentPath)
      f = { kind: 'folder', path, name: parts[parts.length - 1], children: [] }
      folderByPath.set(path, f)
      parent.children.push(f)
      return f
    }
    for (const item of pipelines) {
      const path = getFolderPath(item)
      const folder = ensureFolder(path)
      folder.children.push({ kind: 'leaf', item })
    }
    sortNode(root)
    return root
  })

  // All folders start collapsed. When pipelineName changes, its ancestor
  // folders are auto-expanded. pipelines and expanded are accessed via
  // untrack so data refreshes and user-driven collapses don't re-trigger.
  let expanded = $state(new Set<string>())

  $effect(() => {
    const name = pipelineName // tracked: re-runs only when navigating
    const item = untrack(() => pipelines.find((p) => p.name === name))
    if (!item) {
      return
    }
    const path = getFolderPath(item)
    if (!path) {
      return
    }
    const parts = path.split('/').filter(Boolean)
    const ancestors = parts.map((_, i) => parts.slice(0, i + 1).join('/'))
    untrack(() => {
      const toAdd = ancestors.filter((a) => !expanded.has(a))
      if (toAdd.length > 0) {
        expanded = new Set([...expanded, ...toAdd])
      }
    })
  })

  const isExpanded = (path: string) => expanded.has(path)
  const toggleExpanded = (path: string) => {
    const next = new Set(expanded)
    if (next.has(path)) {
      next.delete(path)
    } else {
      next.add(path)
    }
    expanded = next
  }

  const leafCount = (node: FolderNode): number =>
    node.children.reduce((s, c) => s + (c.kind === 'leaf' ? 1 : leafCount(c)), 0)
</script>

{#snippet renderNode(node: TreeNode, depth: number)}
  {#if node.kind === 'folder'}
    <button
      class="flex w-full items-center gap-1 rounded py-1.5 hover:bg-surface-50-950"
      style="padding-left: {depth * 1.25 + 0.25}rem"
      onclick={() => toggleExpanded(node.path)}
    >
      <span
        class="fd {isExpanded(node.path)
          ? 'fd-chevron-down'
          : 'fd-chevron-right'} flex-none text-[18px] text-surface-500"
      ></span>
      <span class="truncate font-medium">{node.name}</span>
      <span class="ml-auto flex-none pr-1 text-sm text-surface-500">({leafCount(node)})</span>
    </button>
    {#if isExpanded(node.path)}
      {#each node.children as child (child.kind === 'folder' ? `f:${child.path}` : `l:${child.item.name}`)}
        {@render renderNode(child, depth + 1)}
      {/each}
    {/if}
  {:else}
    <a
      class="flex h-9 flex-nowrap items-center justify-between gap-2 rounded py-2 {pipelineName ===
      node.item.name
        ? 'bg-surface-50-950'
        : 'hover:bg-surface-50-950'}"
      style="padding-left: {depth * 1.25 + 1}rem"
      onclick={onaction}
      href={resolve(`/pipelines/${encodeURI(node.item.name)}/`)}
    >
      <span class="min-w-0 overflow-hidden overflow-ellipsis whitespace-nowrap"
        >{node.item.name}</span
      >
      <PipelineStatusDot status={node.item.status}></PipelineStatusDot>
    </a>
  {/if}
{/snippet}

<div class="flex flex-col">
  {#each tree.children as child (child.kind === 'folder' ? `f:${child.path}` : `l:${child.item.name}`)}
    {@render renderNode(child, 0)}
  {/each}
</div>
