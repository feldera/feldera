<!--
  SearchBar: shared "search within the active view" widget for the pipeline Logs tab and the
  profiler bundle viewer. Renders inline as a search-icon button that opens a popup with the query
  input, a match counter, and prev/next nav buttons.

  Presentational only: the host owns the query and runs the search. `results` is the single source
  of truth — the counter, the host's highlight, and the nav buttons all derive from it, and Escape
  / editing / closing call `onclear` so they clear together.

  Hosts open the search by wiring Ctrl/Cmd-F to the exported `activate()`; the widget handles no
  shortcut itself. The popup closes only via `toggle()`, Escape, the close button, or blur when
  empty — never on outside click.

  With `inline`, there is no button or popup: the field is always shown, `activate()` focuses it,
  and Escape or the clear button empties it instead of closing.
-->
<script lang="ts">
  import { errorTextClass } from './statusTone'
  import { tick } from 'svelte'
  import searchIcon from './icons/search.svg?raw'
  import type { SearchDirection, SearchProgress } from './logSearch'

  interface Props {
    /** The query text. Bindable so the host reads what was typed. */
    value: string
    placeholder?: string
    title?: string
    /** The single source of truth for the submitted search — see {@link SearchProgress}.
     *  `null` (no active search) hides the counter and disables the nav buttons; the host must
     *  reset it to `null` on `onclear` so the counter, highlight, and buttons clear together. */
    results?: SearchProgress | null
    /** The underlying input element, exposed so hosts can focus it (e.g. on Ctrl/Cmd-F from
     *  the results view). */
    inputEl?: HTMLInputElement
    /** Whether the popup is open. Bindable so hosts can open it programmatically (Ctrl/Cmd-F). */
    open?: boolean
    /** Extra classes for the outer (relative) container. */
    class?: string
    /** Extra classes for the input (width, ...). */
    inputClass?: string
    /** Extra classes for the search toggle button (size, style preset, ...). */
    buttonClass?: string
    /** Always show the field in place instead of a button that opens a popup. */
    inline?: boolean
    /** Advance to the next match — Enter or the down button. */
    onnext: () => void
    /** Step back to the previous match — Shift-Enter or the up button. */
    onprevious: () => void
    /** Drop the submitted search (highlight + counter + nav). Called on Escape, on edit, and on
     *  close; the host must reset its results to `null`. */
    onclear?: () => void
  }

  let {
    value = $bindable(''),
    placeholder,
    title,
    results = null,
    inputEl = $bindable(),
    open = $bindable(false),
    class: className = '',
    inputClass = '',
    buttonClass = '',
    inline = false,
    onnext,
    onprevious,
    onclear
  }: Props = $props()

  const hasResults = $derived(!!results && results.total > 0)
  const counterText = $derived(
    !results ? '' : results.total > 0 ? `${results.current} of ${results.total}` : 'No results'
  )

  // The element that had the focus when the popup opened; closing returns focus there (with `preventScroll`
  // so the viewport doesn't jump), so keyboard nav resumes where the user left off.
  //
  // The search-icon button uses `onmousedown preventDefault` so clicking it doesn't steal focus:
  // Chrome focuses a <button> on click (Firefox doesn't), which would otherwise make the button
  // the `opener` instead of the content the user was in.
  let opener: HTMLElement | null = null

  // Open the popup and focus/select the query, or just refocus it if already open. Hosts wire
  // Ctrl/Cmd-F here, so the shortcut opens-or-refocuses and never closes the search.
  export async function activate() {
    if (inline) {
      if (document.activeElement !== inputEl) {
        opener = document.activeElement as HTMLElement | null
      }
    } else if (!open) {
      opener = document.activeElement as HTMLElement | null
      open = true
      await tick()
    }
    inputEl?.focus()
    inputEl?.select()
  }

  // Trigger-button click: close if open, else open.
  export async function toggle() {
    if (open) {
      close()
    } else {
      await activate()
    }
  }

  // Clear the field and drop any displayed results (highlight + counter).
  function reset() {
    value = ''
    onclear?.()
  }

  function close() {
    if (inline) {
      reset()
      if (opener) {
        opener.focus({ preventScroll: true })
      } else {
        inputEl?.focus()
      }
      opener = null
      return
    }
    open = false
    reset()
    opener?.focus({ preventScroll: true })
    opener = null
  }

  // Submit the current query and step to the next / previous match.
  function submit(direction: SearchDirection) {
    if (direction === 'prev') {
      onprevious()
    } else {
      onnext()
    }
  }

  function onkeydown(e: KeyboardEvent) {
    if (e.key === 'Enter') {
      e.preventDefault()
      submit(e.shiftKey ? 'prev' : 'next')
    } else if (e.key === 'Escape') {
      close()
    }
  }

  // Editing the query invalidates the displayed results — drop them (the host resets `results`
  // to null, which clears the counter, the highlight, and the nav buttons together).
  function oninput() {
    if (results) {
      onclear?.()
    }
  }

  // An empty field losing focus closes the popup; a field with text stays open so focus can move to
  // the nav / close buttons. No focus-restore here — blur means focus is already moving on.
  function onblur() {
    if (!inline && !value) {
      open = false
      reset()
      opener = null
    }
  }
</script>

{#if inline}
  <div
    class="flex items-center gap-1 rounded border border-line bg-white pr-1 transition-colors focus-within:border-primary-500 dark:bg-dark {className}"
  >
    <span class="flex shrink-0 pl-2 text-[16px] text-surface-600-400" aria-hidden="true">
      {@html searchIcon}
    </span>
    <input
      bind:this={inputEl}
      bind:value
      type="text"
      {placeholder}
      {title}
      {onkeydown}
      {oninput}
      class="min-w-0 flex-1 border-0 bg-transparent px-1 py-0 outline-none focus:ring-0 {inputClass}"
    />
    <!-- Before a search runs, says how to run it; after, the match position or "No results". -->
    {#if results}
      <span
        class="shrink-0 px-1 text-sm whitespace-nowrap {hasResults
          ? 'text-surface-600-400'
          : errorTextClass}"
        aria-live="polite">{counterText}</span
      >
    {:else if value}
      <span class="shrink-0 px-1 text-sm whitespace-nowrap text-surface-600-400">
        Press <kbd class="rounded border border-surface-200-800 px-1 text-xs">Enter</kbd>
      </span>
    {/if}
    <!-- Match navigation shows once a search has run. -->
    <div class="flex shrink-0 items-center gap-1 {results ? '' : 'hidden'}">
      <button
        type="button"
        class="fd fd-arrow-down btn-icon size-5! min-h-0 rotate-180 p-0 text-[14px] hover:not-disabled:bg-row-hover disabled:opacity-30"
        onclick={() => submit('prev')}
        disabled={!hasResults}
        aria-label="Previous match"
        title="Previous match (Shift+Enter)"
      >
      </button>
      <button
        type="button"
        class="fd fd-arrow-down btn-icon size-5! min-h-0 p-0 text-[14px] hover:not-disabled:bg-row-hover disabled:opacity-30"
        onclick={() => submit('next')}
        disabled={!hasResults}
        aria-label="Next match"
        title="Next match (Enter)"
      >
      </button>
    </div>
    {#if value}
      <span class="mx-1 h-4 w-px shrink-0 bg-surface-200-800" aria-hidden="true"></span>
      <button
        type="button"
        class="fd fd-x btn-icon size-5! min-h-0 p-0 text-[14px] hover:bg-row-hover"
        onclick={close}
        aria-label="Clear search"
        title="Clear search (Esc)"
      >
      </button>
    {/if}
  </div>
{:else}
<div class="relative flex items-center {className}">
  <button
    type="button"
    class="btn-icon p-0.5 text-[16px] hover:bg-row-hover {buttonClass}"
    class:preset-tonal-surface={open}
    onclick={toggle}
    onmousedown={(e) => e.preventDefault()}
    aria-label="Search"
    aria-expanded={open}
    {title}
  >
    {@html searchIcon}
  </button>

  {#if open}
    <!-- Anchored just below the button's top-right, growing leftward from its right edge. -->
    <div
      class="absolute top-9 right-1 z-20 flex items-center gap-1 rounded border border-surface-200-800 bg-surface-50-950 p-1 shadow-md"
    >
      <input
        bind:this={inputEl}
        bind:value
        type="text"
        {placeholder}
        {title}
        {onkeydown}
        {oninput}
        {onblur}
        class="input {inputClass}"
      />
      <!-- Fixed-width slot (shrink-0 so flex keeps it reserved even when the text is empty),
           so the nav buttons don't shift as the counter text appears/changes. -->
      <span class="w-14 px-1 shrink-0 text-sm whitespace-nowrap text-surface-600-400 text-right">{counterText}</span>
      <button
        type="button"
        class="fd fd-arrow-down btn-icon rotate-180 p-0 hover:not-disabled:bg-row-hover disabled:opacity-30"
        onclick={() => submit('prev')}
        disabled={!hasResults}
        aria-label="Previous match"
        title="Previous match (Shift+Enter)"
      >
      </button>
      <button
        type="button"
        class="fd fd-arrow-down btn-icon p-0 hover:not-disabled:bg-row-hover disabled:opacity-30"
        onclick={() => submit('next')}
        disabled={!hasResults}
        aria-label="Next match"
        title="Next match (Enter)"
      >
      </button>
      <button
        type="button"
        class="fd fd-x btn-icon p-0 ml-2 hover:bg-row-hover"
        onclick={close}
        aria-label="Close search"
        title="Close search (Esc to clear)"
      >
      </button>
    </div>
  {/if}
</div>
{/if}
