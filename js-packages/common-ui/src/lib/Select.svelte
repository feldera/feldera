<script lang="ts">
  import type { HTMLSelectAttributes } from 'svelte/elements'
  import type { Snippet } from 'svelte'

  /**
   * Drop-in replacement for `<select>` that brands every dropdown in the app with a
   * consistent baseline (`select h-6 bg-surface-100-900/50 border-none`). Accepts the
   * full `HTMLSelectAttributes` surface, so call sites change `<select …>` to
   * `<Select …>` without otherwise touching their props or option children. The caller's
   * `class` is appended to (not overridden by) the default — Tailwind utility ordering
   * gives the caller's classes the final word for conflicting properties.
   *
   * `value` is `$bindable()`, so both `bind:value` and one-way `value=` callers work.
   * The native change/input events still bubble through `{...rest}`, so existing
   * `onchange` handlers continue to receive an event whose `currentTarget` is the
   * underlying `<select>`.
   */
  let {
    value = $bindable(),
    class: className = '',
    children,
    ...rest
  }: HTMLSelectAttributes & { children?: Snippet } = $props()
</script>

<select bind:value class="select text-sm h-6 py-0! bg-surface-100-900/50 ring-0 {className}" {...rest}>
  {@render children?.()}
</select>
