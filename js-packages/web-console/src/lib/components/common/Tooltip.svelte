<script lang="ts" module>
  import Tooltip from './Tooltip.svelte'
  export { Tooltip }
</script>

<script lang="ts">
  import FlowbiteTooltip from 'flowbite-svelte/Tooltip.svelte'
  import { type TooltipProps } from 'flowbite-svelte'

  let {
    children,
    open = $bindable(),
    class: _class,
    ...props
  }: Omit<TooltipProps, 'isOpen'> & { open?: boolean } = $props()
</script>

<FlowbiteTooltip
  {...props}
  class="z-20 rounded-container border-none bg-white p-2 text-base font-normal text-surface-950-50 shadow-sm dark:bg-black {_class}"
>
  {@render children?.()}
</FlowbiteTooltip>
