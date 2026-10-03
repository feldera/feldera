<!--
  @component
  A split button: a main action joined to a chevron that opens more options.

  The caller renders the main action as `children`, because its button often brings its
  own tooltips or responsive variants. Give that button the `.btn` size class that
  matches `size` (`btn-sm`, nothing, or `btn-lg`); the chevron follows `size` itself.
  The `.btn-split` rules in `layout.css` square the inner corners and draw the divider.
  With `variant="outlined"`, give the main button `preset-outlined-surface-200-800` as
  well; the chevron gets it automatically, and the two share one border as divider.

  ```svelte
  <SplitButton ontoggle={toggle} toggleLabel="More options">
    <button class="btn preset-filled-surface-100-900" {onclick}>Run</button>
  </SplitButton>
  ```
-->
<script lang="ts" module>
  export type SplitButtonSize = 'sm' | 'md' | 'lg'
  export type SplitButtonVariant = 'filled' | 'outlined'

  // The chevron button's size class and icon size for each split button size, after the
  // Figma "Icon Button": 24px, 32px and 40px squares with 16px, 16px and 18px icons.
  const toggleClasses: Record<SplitButtonSize, { button: string; icon: string }> = {
    sm: { button: 'btn-icon-sm', icon: 'text-[16px]' },
    md: { button: '', icon: 'text-[16px]' },
    lg: { button: 'btn-icon-lg', icon: 'text-[18px]' }
  }

  // The chevron's style for each variant. A filled chevron takes its colour from
  // `toggleClass`, to match whatever preset the main button uses.
  const variantClasses: Record<SplitButtonVariant, { root: string; toggle: string }> = {
    filled: { root: '', toggle: '' },
    outlined: { root: 'btn-split-outlined', toggle: 'preset-outlined-surface-200-800' }
  }
</script>

<script lang="ts">
  import type { Snippet } from '$lib/types/svelte'

  const {
    children,
    ontoggle,
    toggleLabel,
    size = 'md',
    variant = 'filled',
    toggleClass = '',
    class: _class = ''
  }: {
    /** The main action. Its button's size class should match `size`. */
    children: Snippet
    /** Opens or closes the menu of further options. */
    ontoggle: () => void
    /** The chevron's accessible name, e.g. "See stop options". */
    toggleLabel: string
    size?: SplitButtonSize
    /** `outlined` draws a border instead of a fill, after the Figma outline buttons. */
    variant?: SplitButtonVariant
    /** Classes for the chevron only, typically the colour preset of the main button. */
    toggleClass?: string
    class?: string
  } = $props()

  const toggleButtonClass = $derived(
    [toggleClasses[size].button, variantClasses[variant].toggle, toggleClass].join(' ')
  )
</script>

<div class="btn-split {variantClasses[variant].root} {_class}">
  <div class="flex">
    {@render children()}
  </div>
  <button
    type="button"
    class="btn-split-toggle btn-icon {toggleButtonClass}"
    onclick={ontoggle}
    aria-label={toggleLabel}
  >
    <span class="fd fd-chevron-down {toggleClasses[size].icon}"></span>
  </button>
</div>
