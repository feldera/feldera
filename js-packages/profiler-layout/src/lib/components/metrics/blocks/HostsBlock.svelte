<script lang="ts">
  import { BytesValue, type JsonHostProfile } from 'profiler-lib'
  import { isCollapsed, toggleCollapsed } from '../collapsedBlocks.svelte'
  import BlockTitle from '../parts/BlockTitle.svelte'

  interface Props {
    id: string
    title: string
    /** The hosts that ran the workers, from the profile's `hosts`. */
    hosts: ReadonlyArray<JsonHostProfile>
  }
  const { id, title, hosts }: Props = $props()

  const collapsed = $derived(isCollapsed(id))

  // Each field but `ordinal` and `workers` can be absent: outside Kubernetes, or without cgroup v2.
  const unknown = '—'

  function workerRange(host: JsonHostProfile): string {
    const { start, end } = host.workers
    return end - start === 1 ? `${start}` : `${start}–${end - 1}`
  }

  function cpu(host: JsonHostProfile): string {
    const cores = host.cpu_limit_cores
    return cores === undefined ? unknown : `${cores} ${cores === 1 ? 'core' : 'cores'}`
  }

  function memory(host: JsonHostProfile): string {
    const used = host.memory_used_bytes
    const limit = host.memory_limit_bytes
    if (used === undefined && limit === undefined) {
      return unknown
    }
    const usedText = used === undefined ? unknown : new BytesValue(used).toString()
    if (limit === undefined) {
      return usedText
    }
    const limitText = new BytesValue(limit).toString()
    if (used === undefined || limit === 0) {
      return `${usedText} / ${limitText}`
    }
    return `${usedText} / ${limitText} (${Math.round((used / limit) * 100)}%)`
  }

  const cell = 'px-2 py-1 text-sm text-surface-900-100'
  const header = 'px-2 py-1 text-left text-sm font-medium text-surface-700-300'
</script>

<!-- One row per host (pod) of a multihost pipeline: the workers it ran, where it ran, and its
     cgroup CPU and memory limits. -->
<div class="rounded-base bg-white-dark px-4 py-1.5 shadow-sm" data-block-id={id}>
  <BlockTitle
    class={collapsed ? '' : 'mb-1.5'}
    {title}
    {collapsed}
    onToggle={() => toggleCollapsed(id)}
  />
  {#if !collapsed}
    <div class="scrollbar overflow-x-auto">
      <table class="w-full border-collapse">
        <thead>
          <tr>
            <th class={header}>Host</th>
            <th class={header}>Workers</th>
            <th class={header}>Pod</th>
            <th class={header}>Node</th>
            <th class="{header} text-right">CPU limit</th>
            <th class="{header} text-right">Memory used / limit</th>
          </tr>
        </thead>
        <tbody>
          {#each hosts as host (host.ordinal)}
            <tr class="border-t border-surface-200-800">
              <td class="{cell} tabular-nums">{host.ordinal}</td>
              <td class="{cell} tabular-nums">{workerRange(host)}</td>
              <td class="{cell} break-all font-mono">{host.pod_name ?? unknown}</td>
              <td class="{cell} break-all font-mono">{host.node_name ?? unknown}</td>
              <td class="{cell} text-right tabular-nums">{cpu(host)}</td>
              <td class="{cell} text-right tabular-nums">{memory(host)}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
</div>
