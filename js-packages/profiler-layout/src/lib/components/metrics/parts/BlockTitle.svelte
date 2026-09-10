<script lang="ts">
  interface Props {
    title: string
    collapsed: boolean
    /** When set, the click is refused and the title's tooltip says why. The button keeps its
     * place in the tab order, so the reason is reachable by keyboard and on hover. */
    pinned?: string
    onToggle: () => void
    class?: string
  }
  const { title, collapsed, pinned, onToggle, class: className = '' }: Props = $props()
</script>

<!-- A block's title doubles as its collapse toggle. -->
<h3 class={className}>
  <button
    type="button"
    class="flex items-center gap-1 text-left text-base font-semibold text-surface-900-100"
    class:cursor-default={pinned !== undefined}
    aria-expanded={!collapsed}
    aria-disabled={pinned !== undefined}
    title={pinned ?? (collapsed ? 'Expand' : 'Collapse')}
    onclick={() => pinned === undefined && onToggle()}
  >
    <span
      class="fd fd-chevron-down chevron text-[16px] text-surface-600-400"
      class:rotate-180={!collapsed}
      aria-hidden="true"
    ></span>
    {title}
  </button>
</h3>

<style>
  .chevron {
    display: inline-block;
    transition: transform 200ms ease;
  }
</style>
