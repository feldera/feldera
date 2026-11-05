<script lang="ts">
  import { TagsInput } from '@skeletonlabs/skeleton-svelte'
  import { tick } from 'svelte'
  import type { Attachment } from 'svelte/attachments'
  import TagChip, { tagChipClass } from '#lib/components/pipelines/table/TagChip.svelte'
  import { findMatchingTags } from '#lib/functions/pipelines/search.ts'
  import { tagColorOf, tagDisplayName } from '#lib/functions/pipelines/tags.ts'

  let {
    pipelines,
    query = $bindable(''),
    tagFilters = $bindable([]),
    tagsLayout,
    class: _class = '',
    inputClass = ''
  }: {
    /** The pipelines whose tags the search suggests. */
    pipelines: { name: string; tags: string[] }[]
    /** The text typed in the search input. */
    query?: string
    /** The picked tags, in the `name|rrggbb` form. */
    tagFilters?: string[]
    /**
     * The place of the list of tags that match the query. The list shows while the input has focus.
     * - 'above': a popup above the input, with rows that wrap.
     * - 'right': a popup to the right of the input, with one row.
     * - 'below': a list under the input, in the page layout, with rows that wrap.
     */
    tagsLayout: 'above' | 'right' | 'below'
    class?: string
    inputClass?: string
  } = $props()

  const pickedTagNames = $derived(new Set(tagFilters.map(tagDisplayName)))
  const suggestedTags = $derived(
    findMatchingTags(pipelines, query).filter((tag) => !pickedTagNames.has(tagDisplayName(tag)))
  )
  // The suggested tag that the keyboard highlights.
  // Tab and Shift+Tab set it, and typing clears it.
  let highlightedTag = $state<string>()

  // TagsInput adds a tag when the typed text ends with the delimiter (',' by default),
  // and then clears the input. An empty delimiter prevents this, so that commas stay in the query.
  const NO_DELIMITER = ''

  const pickTag = (tag: string) => {
    tagFilters = [...tagFilters, tag]
    query = ''
  }

  // Tab and Shift+Tab move the highlight through the suggested tags.
  // Enter picks the highlighted tag, or else the suggested tag whose name is the typed text.
  // This handler prevents the default of every Enter, because TagsInput clears the input on Enter.
  const handleKeyDown = async (e: KeyboardEvent) => {
    if (e.key === 'Enter') {
      e.preventDefault()
      const typedName = query.trim()
      const tag = highlightedTag ?? suggestedTags.find((tag) => tagDisplayName(tag) === typedName)
      if (tag) {
        pickTag(tag)
      }
      return
    }
    if (e.key === 'Tab' && suggestedTags.length > 0) {
      e.preventDefault()
      const count = suggestedTags.length
      const step = e.shiftKey ? -1 : 1
      // Without a highlight, start one step before the first tag or after the last tag.
      const startIndex = e.shiftKey ? count : -1
      const current = highlightedTag ? suggestedTags.indexOf(highlightedTag) : startIndex
      highlightedTag = suggestedTags[(current + step + count) % count]
      await tick()
      tagList
        ?.querySelector('[data-highlighted]')
        ?.scrollIntoView({ block: 'nearest', inline: 'nearest' })
    }
  }

  let isInputFocused = $state(false)
  let inputRow = $state<HTMLElement>()
  let inputRowRect = $state<DOMRect>()
  let tagList = $state<HTMLElement>()
  const isTagListOpen = $derived(isInputFocused && suggestedTags.length > 0)
  const isPopup = $derived(tagsLayout !== 'below')

  // After a pointer down outside TagsInput, TagsInput ignores typing, but the input keeps
  // the DOM focus. The tag list is outside TagsInput, so ignore pointer downs on the list.
  const keepFocusOnTagListClick = (e: CustomEvent<{ target: EventTarget }>) => {
    if (tagList?.contains(e.detail.target as Node)) {
      e.preventDefault()
    }
  }

  // Follow the input while the popup is open, because the page can scroll or resize.
  $effect(() => {
    if (!isTagListOpen || !isPopup || !inputRow) {
      return
    }
    const row = inputRow
    const measure = () => (inputRowRect = row.getBoundingClientRect())
    measure()
    window.addEventListener('resize', measure)
    window.addEventListener('scroll', measure, true)
    return () => {
      window.removeEventListener('resize', measure)
      window.removeEventListener('scroll', measure, true)
    }
  })

  const POPUP_GAP_PX = 4
  const SCREEN_MARGIN_PX = 8

  // Place the popup next to the input. Limit the popup size to the space left on screen,
  // so that the popup scrolls when the tags do not fit.
  const popupStyle = $derived.by(() => {
    if (!isPopup || !inputRowRect) {
      return ''
    }
    const rect = inputRowRect
    const screenWidth = document.documentElement.clientWidth
    const screenHeight = document.documentElement.clientHeight
    if (tagsLayout === 'right') {
      const left = rect.right + POPUP_GAP_PX
      return `left: ${left}px; top: ${rect.top + rect.height / 2}px; transform: translateY(-50%); max-width: ${screenWidth - left - SCREEN_MARGIN_PX}px;`
    }
    return `left: ${rect.left}px; bottom: ${screenHeight - rect.top + POPUP_GAP_PX}px; max-width: min(var(--container-lg), ${screenWidth - rect.left - SCREEN_MARGIN_PX}px); max-height: ${rect.top - POPUP_GAP_PX - SCREEN_MARGIN_PX}px;`
  })

  const popupClass = 'bg-white-dark fixed z-50 w-max rounded p-2 shadow-md'
  const tagListClass = {
    above: `${popupClass} flex-wrap overflow-y-auto`,
    right: `${popupClass} overflow-x-auto`,
    below: 'mt-2 max-h-40 flex-wrap overflow-y-auto'
  }

  // A mouse wheel scrolls vertically, so turn it into a horizontal scroll of the row.
  const scrollRowWithWheel = (e: WheelEvent) => {
    if (tagsLayout !== 'right' || e.deltaY === 0) {
      return
    }
    e.preventDefault()
    ;(e.currentTarget as HTMLElement).scrollLeft += e.deltaY
  }

  // An ancestor with `container-type` is the containing block of fixed elements, and
  // the scroll area of that ancestor clips the popup. Mount the popup in <body>.
  const mountInBody: Attachment<HTMLElement> = (node) => {
    document.body.appendChild(node)
    return () => node.remove()
  }
</script>

<div class="min-w-0 {_class}" bind:this={inputRow}>
  <TagsInput
    value={tagFilters}
    onValueChange={(details) => (tagFilters = details.value)}
    inputValue={query}
    onInputValueChange={(details) => {
      query = details.inputValue
      highlightedTag = undefined
    }}
    onInteractOutside={keepFocusOnTagListClick}
    delimiter={NO_DELIMITER}
    editable={false}
    addOnPaste={false}
  >
    <TagsInput.Control class="input {inputClass}">
      <div class="flex min-w-0 flex-wrap items-center gap-1">
        {#each tagFilters as tag, index (tag)}
          <!-- `min-w-0 max-w-full` on each wrapper lets a long tag shrink to the input
               width, so that its text ends in an ellipsis. -->
          <TagsInput.Item value={tag} {index} class="flex max-w-full min-w-0">
            <!-- Skeleton styles these parts as a filled chip with extra padding.
                 These classes give them the look of the tag chips in the table. -->
            <TagsInput.ItemPreview
              class="{tagChipClass} max-w-full min-w-0 bg-transparent text-inherit"
            >
              <span
                class="h-2 w-2 shrink-0 rounded-full"
                style="background-color: {tagColorOf(tag)}"
              ></span>
              <TagsInput.ItemText class="min-w-0 truncate p-0 leading-[inherit]"
                >{tagDisplayName(tag)}</TagsInput.ItemText
              >
              <TagsInput.ItemDeleteTrigger
                class="fd fd-x shrink-0 p-0 text-[12px] text-surface-600-400 hover:bg-transparent hover:text-surface-950-50"
                aria-label="Remove tag filter"
                data-testid="btn-remove-tag-filter-{tagDisplayName(tag)}"
              >
                {#snippet children()}{/snippet}
              </TagsInput.ItemDeleteTrigger>
            </TagsInput.ItemPreview>
          </TagsInput.Item>
        {/each}
        <TagsInput.Input
          data-testid="input-pipeline-search"
          class="min-w-16 flex-1 input-ghost"
          placeholder={tagFilters.length ? '' : 'Search pipelines and tags...'}
          onfocus={() => (isInputFocused = true)}
          onblur={() => (isInputFocused = false)}
          onkeydowncapture={handleKeyDown}
        />
      </div>
    </TagsInput.Control>
    <TagsInput.HiddenInput />
  </TagsInput>
  {#if isTagListOpen}
    <!-- Prevent the default on mousedown, so that the input keeps the focus and the
         tag list stays open until the click on a tag. -->
    <div
      {@attach isPopup && mountInBody}
      bind:this={tagList}
      class="scrollbar flex gap-1 {tagListClass[tagsLayout]}"
      style={popupStyle}
      data-testid="box-tag-suggestions"
      role="presentation"
      onmousedown={(e) => e.preventDefault()}
      onwheel={scrollRowWithWheel}
    >
      {#each suggestedTags as tag (tag)}
        <TagChip
          {tag}
          onclick={() => pickTag(tag)}
          class={tagsLayout === 'right' ? '' : 'max-w-full min-w-0'}
          isHighlighted={tag === highlightedTag}
          data-testid="btn-tag-suggestion-{tagDisplayName(tag)}"
        ></TagChip>
      {/each}
    </div>
  {/if}
</div>
