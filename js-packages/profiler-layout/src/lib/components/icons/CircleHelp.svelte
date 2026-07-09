<script lang="ts">
  // Inlined circle-help icon (from feldera-material-icons) so it bundles with the library rather
  // than relying on a consumer-served static asset. Strokes with `currentColor` to inherit theme.
  interface Props {
    size?: number
  }
  const { size = 16 }: Props = $props()
</script>

<svg
  xmlns="http://www.w3.org/2000/svg"
  width={size}
  height={size}
  fill="none"
  viewBox="0 0 24 24"
  aria-hidden="true"
>
  <path
    stroke="currentColor"
    stroke-linecap="round"
    stroke-linejoin="round"
    stroke-width="2"
    d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3m.08 4h.01M22 12c0 5.523-4.477 10-10 10S2 17.523 2 12 6.477 2 12 2s10 4.477 10 10Z"
  />
</svg>
