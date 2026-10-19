<!--
  @component
  A Skeleton `Switch` smaller than its 40 × 24px default, after the Figma "Switch".
  `sm` is 28 × 16px, for use beside 16px checkboxes; `md` is 34 × 20px, for settings lists.
-->
<script lang="ts">
  import { Switch } from '@skeletonlabs/skeleton-svelte'
  import type { ComponentProps } from 'svelte'

  let { size, ...props }: { size: 'sm' | 'md' } & ComponentProps<typeof Switch> = $props()

  // The thumb fills the control's height. Skeleton slides the checked thumb by
  // `--spacing(4)`, so a smaller `--spacing` makes it travel
  // width - 2 × padding - thumb: 12px for `sm`, 14px for `md`.
  const styles = {
    sm: {
      control: 'h-4 w-7 p-px',
      thumb: '[--spacing:3px] shadow-[0_1px_2px_rgb(0_0_0/0.15)]'
    },
    md: {
      control: 'h-5 w-8.5 p-0.5',
      thumb: '[--spacing:3.5px]'
    }
  }
</script>

<Switch {...props}>
  <Switch.Control class={styles[size].control}>
    <Switch.Thumb class={styles[size].thumb} />
  </Switch.Control>
  <Switch.Label />
  <Switch.HiddenInput />
</Switch>
