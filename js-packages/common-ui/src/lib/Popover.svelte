<script lang="ts" module>
  import Popover from './Popover.svelte'
  export { Popover }
</script>

<script lang="ts">
  import { Popover as FlowbitePopover } from 'flowbite-svelte'
  import { type PopoverProps } from 'flowbite-svelte'

  let { children, isOpen = $bindable(false), ...props }: PopoverProps = $props()
  const { class: _class, ...otherProps } = { class: '', ...props, title: undefined }
</script>

<FlowbitePopover
  class="bg-white-dark z-20 rounded-container border-none p-2 text-base font-normal text-surface-950-50 {_class}"
  {...otherProps}
  bind:isOpen
>
  {@render children?.()}
</FlowbitePopover>
