<script lang="ts">
  import { TagsInput } from '@skeletonlabs/skeleton-svelte'
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
    /** The tags picked from the suggestions. */
    tagFilters?: string[]
    /**
     * Where the tags that match the query show while the input has focus:
     * 'above' - in a popup with a wrapping row above the input;
     * 'right' - in a popup with a single row to the right of the input;
     * 'below' - in a wrapping row under the input, inside the page layout.
     */
    tagsLayout: 'above' | 'right' | 'below'
    class?: string
    inputClass?: string
  } = $props()

  const pickedTagNames = $derived(new Set(tagFilters.map(tagDisplayName)))
  const suggestedTags = $derived(
    findMatchingTags(pipelines, query).filter((tag) => !pickedTagNames.has(tagDisplayName(tag)))
  )

  const pickTag = (tag: string) => {
    tagFilters = [...tagFilters, tag]
    query = ''
  }

  // TagsInput adds the typed text as a tag on Enter. Accept it only when it is the full
  // text of a tag that is not picked yet, so that free text stays a search query.
  const isPickableTag = ({ inputValue }: { inputValue: string }) =>
    !pickedTagNames.has(tagDisplayName(inputValue)) &&
    pipelines.some((pipeline) => pipeline.tags.includes(inputValue))

  let isInputFocused = $state(false)
  let inputRow = $state<HTMLElement>()
  let inputRowRect = $state<DOMRect>()
  let tagList = $state<HTMLElement>()
  const isTagListOpen = $derived(isInputFocused && suggestedTags.length > 0)
  const isPopupOpen = $derived(isTagListOpen && tagsLayout !== 'below')

  // TagsInput stops reacting to typing after a pointer down outside of it. The tag list
  // is outside of it, so a click on a tag would leave the input focused but ignoring keys.
  const keepFocusOnTagListClick = (e: CustomEvent<{ target: EventTarget }>) => {
    if (tagList?.contains(e.detail.target as Node)) {
      e.preventDefault()
    }
  }

  // Follow the input while the popup is open, because the page can scroll or resize.
  $effect(() => {
    if (!isPopupOpen || !inputRow) {
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

  // Place the popup next to the input and limit its size to the room left on screen;
  // past that, the popup scrolls.
  const popupStyle = $derived.by(() => {
    if (!inputRowRect) {
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

  // A mouse wheel scrolls vertically, so turn it into a horizontal scroll of the row.
  const scrollRowWithWheel = (e: WheelEvent) => {
    if (tagsLayout !== 'right' || e.deltaY === 0) {
      return
    }
    e.preventDefault()
    ;(e.currentTarget as HTMLElement).scrollLeft += e.deltaY
  }

  // Prevent the default on mousedown so the input keeps focus and the tag list stays
  // open until the click on a tag lands.
  const tagListAttributes = {
    'data-testid': 'box-tag-suggestions',
    role: 'presentation',
    onmousedown: (e: MouseEvent) => e.preventDefault()
  }

  // A container query on an ancestor makes it the containing block of a fixed
  // element, and its scroll area then clips the popup. Mount the popup in <body>.
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
    onInputValueChange={(details) => (query = details.inputValue)}
    validate={isPickableTag}
    onInteractOutside={keepFocusOnTagListClick}
    editable={false}
    addOnPaste={false}
  >
    <TagsInput.Control class="input {inputClass}">
      <div class="flex min-w-0 flex-wrap items-center gap-1">
        {#each tagFilters as tag, index (tag)}
          <!-- `min-w-0 max-w-full` down the chain lets a long tag shrink to the input's
               width, so its text ends in an ellipsis instead of overflowing. -->
          <TagsInput.Item value={tag} {index} class="flex max-w-full min-w-0">
            <!-- Skeleton styles these parts by default (a filled chip, extra padding);
                 the classes here restore the look of the tag chips in the table. -->
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
        />
      </div>
    </TagsInput.Control>
    <TagsInput.HiddenInput />
  </TagsInput>
  {#if tagsLayout === 'below' && isTagListOpen}
    <div
      bind:this={tagList}
      class="mt-2 scrollbar flex max-h-40 flex-wrap gap-1 overflow-y-auto"
      {...tagListAttributes}
    >
      {@render tagChips()}
    </div>
  {/if}
</div>

{#if isPopupOpen}
  <div
    {@attach mountInBody}
    bind:this={tagList}
    class="bg-white-dark fixed z-50 scrollbar flex w-max gap-1 rounded p-2 shadow-md {tagsLayout ===
    'above'
      ? 'flex-wrap overflow-y-auto'
      : 'overflow-x-auto'}"
    style={popupStyle}
    onwheel={scrollRowWithWheel}
    {...tagListAttributes}
  >
    {@render tagChips()}
  </div>
{/if}

{#snippet tagChips()}
  {#each suggestedTags as tag (tag)}
    <TagChip
      {tag}
      onclick={() => pickTag(tag)}
      class={tagsLayout === 'right' ? '' : 'max-w-full min-w-0'}
      data-testid="btn-tag-suggestion-{tagDisplayName(tag)}"
    ></TagChip>
  {/each}
{/snippet}
