<script lang="ts" module>
  import Popover from './Popover.svelte'
  export { Popover }
</script>

<script lang="ts">
  import FlowbitePopover from 'flowbite-svelte/Popover.svelte'
  import { type PopoverProps } from 'flowbite-svelte'

  let { children, ...props }: Omit<PopoverProps, 'isOpen'> = $props()
  const { class: _class, ...otherProps } = { class: '', ...props, title: undefined }
</script>

<FlowbitePopover
  class="bg-white-dark z-20 rounded-container border-none p-2 text-base font-normal text-surface-950-50 {_class}"
  {...otherProps}
>
  {@render children?.()}
</FlowbitePopover>
