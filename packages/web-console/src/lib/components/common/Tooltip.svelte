<script lang="ts" module>
  import Tooltip from './Tooltip.svelte'
  export { Tooltip }
</script>

<script lang="ts">
  import FlowbiteTooltip, { type TooltipProps } from 'flowbite-svelte/Tooltip.svelte'

  let { children, open = $bindable(), ...props }: TooltipProps = $props()
</script>

<FlowbiteTooltip {...props} bind:open>
  {@render children?.()}
</FlowbiteTooltip>
