<script lang="ts">
  import { Tooltip } from 'common-ui'
  import { slide } from 'svelte/transition'
  import Popup from '$lib/components/common/Popup.svelte'
  import SlidingPanels from '$lib/components/common/SlidingPanels.svelte'
  import {
    usePipelineList,
    useUpdatePipelineList
  } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import type { PipelineManagerApi } from '$lib/compositions/usePipelineManager.svelte'
  import { useToast } from '$lib/compositions/useToastNotification'
  import { partition } from '$lib/functions/common/array'
  import { promisePool } from '$lib/functions/common/promise'
  import {
    parseTag,
    tagColorOf,
    tagColorPalette,
    tagDisplayName,
    tagWithColor,
    validateTag
  } from '$lib/functions/pipelines/tags'
  import { patchPipeline } from '$lib/services/pipelineManager'

  let {
    pipelineName,
    tags,
    knownTags,
    api
  }: {
    /** Name of the pipeline these tags belong to. */
    pipelineName: string
    /** Tags currently applied to this pipeline. */
    tags: string[]
    /** Every tag known across all pipelines — the pool the popup picks from. */
    knownTags: Set<string>
    /** Pipeline Manager client used to persist tag edits. */
    api: PipelineManagerApi
  } = $props()

  // API returns tags already sorted - every client sorts before writing, so a
  // pipeline's stored tag list is stable regardless of insertion order.
  // This comparator is used only to
  // (1) re-establish that order when writing edited tags and
  // (2) order the combined assigned/unassigned picker,
  // which is built from the unordered cross-pipeline tag pool.
  const byText = (a: string, b: string) => a.localeCompare(b)

  // Tag edits update the pipeline optimistically and patch the server; the next
  // list refresh reconciles with the server, which owns `tags`.
  const { updatePipeline, discardPendingListRefresh } = useUpdatePipelineList()
  const pipelineList = usePipelineList()
  const { toastError } = useToast()

  // Sort the edited tags to store them in the backend,
  // cache that same order optimistically, and return it as the patch body.
  // This is the one place the client orders tags; readers trust the result.
  const applyTagsLocally = (pipelineName: string, next: string[]) => {
    const sorted = [...next].sort(byText)
    updatePipeline(pipelineName, (p) => ({ ...p, tags: sorted }))
    return sorted
  }

  // Persist a single pipeline's tags.
  // `api.patchPipeline` reports errors that may arise to the users in a toast popup.
  const setPipelineTags = (name: string, next: string[]) => {
    api.patchPipeline(name, { tags: applyTagsLocally(name, next) })
  }
  const setTags = (next: string[]) => {
    discardPendingListRefresh()
    setPipelineTags(pipelineName, next)
  }

  // Patch the pipelines through a fixed-size pool of concurrent requests, so a tag shared by
  // hundreds of pipelines doesn't fire hundreds of requests at once. These
  // patches use the bare service call rather than `api.patchPipeline` so we
  // control how failures surface: the first couple of failures toast
  // individually as they land, but the moment we reach `PATCH_FAILURE_TOAST_LIMIT`
  // failures we give up - skip the remaining pipelines, let the in-flight requests
  // settle, then raise one toast covering the remaining failures plus
  // every skipped update.
  const PATCH_CONCURRENCY = 16
  const PATCH_FAILURE_TOAST_LIMIT = 3
  const setManyPipelineTags = async (
    tagName: string,
    updates: { name: string; tags: string[] }[]
  ) => {
    discardPendingListRefresh()
    let failures = 0
    let limitFailureBody = ''
    let stopped = false
    const outcomes = await promisePool(updates, PATCH_CONCURRENCY, async ({ name, tags }) => {
      // Once we've given up, skip without issuing a request. The pool still
      // visits the remaining items cheaply; we count the skips below.
      if (stopped) {
        return 'skipped' as const
      }
      try {
        await patchPipeline(name, { tags: applyTagsLocally(name, tags) })
        return 'ok' as const
      } catch (error) {
        // Toast each failure as it lands, up to the limit. The error that
        // reaches the limit is carried into the aggregate toast below; later
        // failures are only counted.
        failures += 1
        if (failures < PATCH_FAILURE_TOAST_LIMIT) {
          toastError('')(new Error(`Failed to update tag “${tagName}” on pipeline “${name}”`))
        } else if (failures === PATCH_FAILURE_TOAST_LIMIT) {
          limitFailureBody = error instanceof Error ? error.message : String(error)
          stopped = true
        }
        return 'failed' as const
      }
    })
    if (failures >= PATCH_FAILURE_TOAST_LIMIT) {
      // The first `PATCH_FAILURE_TOAST_LIMIT - 1` failures toasted individually;
      // fold the failures past that point together with every update we skipped
      // into one count — annotated with the error that hit the limit.
      const skipped = outcomes.filter((outcome) => outcome === 'skipped').length
      const count = failures - (PATCH_FAILURE_TOAST_LIMIT - 1) + skipped
      toastError('')(
        new Error(
          `Failed to update tag "${tagName}" on ${count} pipeline${count === 1 ? '' : 's'}:\n${limitFailureBody}`
        )
      )
    }
  }
  const toggleTag = (tag: string) => {
    if (tags.includes(tag)) {
      setTags(tags.filter((t) => t !== tag))
      return
    }
    // Assigning a tag drops every other color variant of the same name, so a
    // pipeline never carries two variants. This also cleans up a pipeline that
    // another client gave several variants: checking one unassigns the rest.
    const name = tagDisplayName(tag)
    setTags([...tags.filter((t) => tagDisplayName(t) !== name), tag])
  }
  const createTag = (tag: string) => {
    // Like `toggleTag`, drop any other color variant of the same name so the
    // pipeline never ends up carrying two variants of one tag.
    const name = tagDisplayName(tag)
    setTags([...tags.filter((t) => tagDisplayName(t) !== name), tag])
  }

  // At most two tags fit in the row; the rest collapse into a "+N" chip.
  const inlineTags = $derived(tags.slice(0, 2))
  const overflowCount = $derived(Math.max(0, tags.length - 2))

  let page = $state<'list' | 'create' | 'edit' | 'delete'>('list')
  let search = $state('')
  let newTagName = $state('')
  // The '#rrggbb' color the user has chosen for the tag being created or edited;
  // encoded as a '|rrggbb' suffix on submit (see `tagWithColor`). Defaults to the
  // first palette entry so a freshly created tag always has a color.
  let newTagColor = $state<string>(tagColorPalette[0].color)
  // The exact tag string being edited (with its color suffix), or null when the
  // form is in "create" mode.
  let editingTag = $state<string | null>(null)

  const matchesSearch = (text: string) =>
    tagDisplayName(text).toLowerCase().includes(search.trim().toLowerCase())

  // Selected tags float to the top; both groups are sorted a–z (see design).
  const [selectedTags, unselectedTags] = $derived(
    partition([...knownTags].filter(matchesSearch), (t) => tags.includes(t)).map((group) =>
      group.sort(byText)
    )
  )

  // In create mode, a name matching an existing tag must reuse that tag's color;
  // otherwise we would create two tags with the same name but different colors.
  const existingSameName = $derived(
    [...knownTags].find((tag) => tagDisplayName(tag) === newTagName.trim())
  )
  // The existing tag's actual color, if it has one. An uncolored same-name tag
  // leaves this undefined, so the picker stays open and the user can give the tag
  // a color rather than being locked to the default.
  const createLockedColor = $derived(
    existingSameName === undefined ? undefined : parseTag(existingSameName).color
  )

  // The tag the create page would produce, with the color encoded as a suffix. A
  // tag that already exists (same name *and* same color) is applied rather than
  // created — hence the button reads "Assign" instead of "Create".
  const candidateTag = $derived(tagWithColor(newTagName.trim(), createLockedColor ?? newTagColor))
  const candidateExists = $derived(knownTags.has(candidateTag))

  // Validate the encoded tag against the backend's rules so a bad name is caught
  // here, before the request. Only meaningful once the user has typed a name; an
  // empty name is already handled by disabling submit.
  const candidateError = $derived(newTagName.trim() ? validateTag(candidateTag) : null)

  const submitCreate = () => {
    if (!newTagName.trim()) {
      return
    }
    createTag(candidateTag)
    closeForm()
  }

  const openCreate = (tag: string) => {
    editingTag = null
    newTagName = tagDisplayName(tag)
    newTagColor = parseTag(tag).color ?? tagColorPalette[0].color
    page = 'create'
  }

  const openEdit = (tag: string) => {
    editingTag = tag
    newTagName = tagDisplayName(tag)
    newTagColor = parseTag(tag).color ?? tagColorPalette[0].color
    page = 'edit'
  }

  // Apply an edit to an existing tag. When the tag string changes (a new color or
  // name), every pipeline that carries the old tag is migrated to the new one. The
  // pipeline list is captured up front so a concurrent poll refresh can't change
  // what we iterate over mid-migration.
  const submitEdit = () => {
    const oldTag = editingTag
    if (!oldTag || !newTagName.trim()) {
      return
    }
    const newTag = tagWithColor(newTagName.trim(), newTagColor)
    if (newTag !== oldTag) {
      const pipelines = pipelineList.pipelines ?? []
      setManyPipelineTags(
        tagDisplayName(newTag),
        pipelines
          .filter((pipeline) => pipeline.tags.includes(oldTag))
          .map((pipeline) => ({
            name: pipeline.name,
            // Drop the old tag (and any pre-existing copy of the new one) before
            // adding the new tag, so a pipeline that already had both never ends
            // up with the new tag twice.
            tags: [...pipeline.tags.filter((t) => t !== oldTag && t !== newTag), newTag]
          }))
      )
    }
    closeForm()
  }

  // How many pipelines currently carry the tag being edited — surfaced in the
  // delete confirmation so the user knows the blast radius.
  const editingTagUsageCount = $derived.by(() => {
    if (editingTag === null) {
      return 0
    }
    const tag = editingTag
    return (pipelineList.pipelines ?? []).filter((p) => p.tags.includes(tag)).length
  })

  // Delete the tag being edited: drop it from every pipeline that carries it. As
  // with editing, the pipeline list is captured up front so a concurrent poll
  // refresh can't change what we iterate over mid-deletion.
  const submitDelete = () => {
    const tag = editingTag
    if (!tag) {
      return
    }
    const pipelines = pipelineList.pipelines ?? []
    setManyPipelineTags(
      tagDisplayName(tag),
      pipelines
        .filter((pipeline) => pipeline.tags.includes(tag))
        .map((pipeline) => ({
          name: pipeline.name,
          tags: pipeline.tags.filter((t) => t !== tag)
        }))
    )
    closeForm()
  }

  // Return the form to a clean list view. Also used when the popup (re)opens.
  const closeForm = () => {
    page = 'list'
    search = ''
    newTagName = ''
    editingTag = null
  }
</script>

<Popup>
  {#snippet trigger(toggle)}
    {@const open = () => {
      closeForm()
      toggle()
    }}
    <div class="flex flex-nowrap items-center gap-1">
      {#each inlineTags as tag (tag)}
        {@render chip(tag, open)}
      {/each}
      {#if overflowCount > 0}
        <button
          class="px-1 text-sm text-surface-600-400 hover:text-surface-950-50"
          onclick={open}
          aria-label="Show all tags"
        >
          +{overflowCount}
        </button>
        <Tooltip placement="top" class="max-w-[240px] text-wrap"
          >{tags.map(tagDisplayName).join(', ')}</Tooltip
        >
      {/if}
      {#if tags.length === 0}
        <button
          class="flex h-5 items-center gap-1 rounded border border-dashed border-surface-500 px-2 text-sm text-surface-800-200 hover:border-surface-950-50 hover:text-surface-950-50"
          onclick={open}
        >
          <span class="fd fd-plus text-[16px]"></span>
          Tag
        </button>
      {/if}
    </div>
  {/snippet}
  {#snippet content()}
    <div
      transition:slide={{ duration: 100 }}
      class="bg-white-dark absolute top-8 left-0 z-30 flex w-[304px] flex-col overflow-hidden rounded shadow-md"
    >
      <SlidingPanels
        current={page}
        width={280}
        pages={[
          { key: 'list', content: listPage },
          { key: 'create', content: createPage },
          { key: 'edit', content: editPage },
          { key: 'delete', content: deletePage }
        ]}
      />
    </div>

    {#snippet listPage()}
      <div class="p-2">
        <input class="input h-9 w-full" type="search" placeholder="Search" bind:value={search} />
      </div>
      <div class="scrollbar flex max-h-[280px] flex-col overflow-y-auto pb-1">
        {#each selectedTags as tag (tag)}
          {@render tagRow(tag, true)}
        {/each}
        {#each unselectedTags as tag (tag)}
          {@render tagRow(tag, false)}
        {/each}
      </div>
      <button
        class="flex items-center gap-2 border-t border-surface-100-900 px-3 py-2 text-left text-sm hover:bg-surface-50-950"
        onclick={() => openCreate(search)}
      >
        <span class="fd fd-plus text-[16px]"></span>
        Create a new tag
      </button>
    {/snippet}

    {#snippet createPage()}
      {@render tagForm({
        title: 'Create a new tag',
        submitLabel: candidateExists ? 'Assign' : 'Create',
        submitDisabled: !newTagName.trim() || candidateError !== null,
        lockedColor: createLockedColor,
        validationError: candidateError,
        onSubmit: submitCreate
      })}
    {/snippet}

    {#snippet editPage()}
      {@render tagForm({
        title: 'Edit tag',
        submitLabel: 'Save',
        submitDisabled: !newTagName.trim() || candidateError !== null,
        validationError: candidateError,
        onSubmit: submitEdit,
        onDelete: () => (page = 'delete')
      })}
    {/snippet}

    {#snippet deletePage()}
      <div class="flex items-center gap-2 px-2 py-2">
        <button
          class="btn-icon h-7 w-7"
          onclick={() => (page = 'edit')}
          aria-label="Back"
          title="Back"
        >
          <span class="fd fd-chevron-left text-[20px]"></span>
        </button>
        <span class="text-sm font-medium">Delete tag</span>
      </div>
      <div class="flex flex-col gap-3 px-3 pb-3">
        <p class="">
          The tag “{tagDisplayName(editingTag ?? '')}” will be removed from
          {editingTagUsageCount}
          {editingTagUsageCount === 1 ? 'pipeline' : 'pipelines'} it's currently assigned to.
          <br />
          This cannot be undone.
        </p>
        <div class="flex gap-2">
          <button class="btn flex-1 preset-tonal-surface" onclick={() => (page = 'edit')}>
            Cancel
          </button>
          <button class="btn flex-1 preset-filled-error-500" onclick={submitDelete}>
            Delete
          </button>
        </div>
      </div>
    {/snippet}
  {/snippet}
</Popup>

{#snippet chip(tag: string, open: () => void)}
  <button
    class="flex h-5 items-center gap-1.5 rounded border border-surface-200-800 px-2 text-sm whitespace-nowrap hover:bg-surface-50-950"
    onclick={open}
  >
    <span class="h-2 w-2 rounded-full" style="background-color: {tagColorOf(tag)}"></span>
    {tagDisplayName(tag)}
  </button>
{/snippet}

{#snippet tagRow(tag: string, selected: boolean)}
  <div class="group/row flex items-center hover:bg-surface-50-950">
    <button
      class="flex flex-1 items-center gap-2 py-1.5 pl-3 text-left text-sm"
      onclick={() => toggleTag(tag)}
    >
      <input
        class="pointer-events-none checkbox"
        type="checkbox"
        checked={selected}
        tabindex="-1"
      />
      <span class="h-2.5 w-2.5 shrink-0 rounded-full" style="background-color: {tagColorOf(tag)}"
      ></span>
      <span class="truncate">{tagDisplayName(tag)}</span>
    </button>
    <button
      class="invisible flex h-7 w-7 shrink-0 items-center justify-center text-surface-600-400 group-hover/row:visible hover:text-surface-950-50"
      onclick={() => openEdit(tag)}
      aria-label="Edit tag"
      title="Edit tag"
    >
      <span class="fd fd-settings text-[16px]"></span>
    </button>
  </div>
{/snippet}

{#snippet tagForm(opts: {
  title: string
  submitLabel: string
  submitDisabled: boolean
  lockedColor?: string
  validationError?: string | null
  onSubmit: () => void
  onDelete?: () => void
})}
  {@const selectedColor = opts.lockedColor ?? newTagColor}
  {@const colorName = tagColorPalette.find((c) => c.color === selectedColor)?.name ?? 'Custom'}
  <div class="flex items-center gap-2 px-2 py-2">
    <button class="btn-icon h-7 w-7" onclick={() => (page = 'list')} aria-label="Back" title="Back">
      <span class="fd fd-chevron-left text-[20px]"></span>
    </button>
    <span class="text-sm font-medium">{opts.title}</span>
    {#if opts.onDelete}
      <button
        class="ml-auto btn-icon h-7 w-7 text-surface-600-400 hover:text-error-500"
        onclick={opts.onDelete}
        aria-label="Delete tag"
        title="Delete tag"
      >
        <span class="fd fd-trash-2 text-[18px]"></span>
      </button>
    {/if}
  </div>
  <div class="flex flex-col gap-3 px-3 pb-3">
    <label class="flex flex-col gap-1 text-sm font-medium">
      Tag name
      <input
        class="input h-9 w-full"
        class:border-error-500={!!opts.validationError}
        type="text"
        bind:value={newTagName}
        placeholder="Tag name"
      />
      {#if opts.validationError}
        <span class="text-xs font-normal text-error-500">{opts.validationError}</span>
      {/if}
    </label>
    <div class="flex flex-col gap-1 text-sm font-medium">
      <span class="flex items-center gap-2">
        Label color
        <span class="font-normal text-surface-500">{colorName}</span>
      </span>
      <div class="flex flex-wrap gap-2 py-1">
        {#each tagColorPalette as paletteColor (paletteColor.name)}
          <button
            type="button"
            disabled={opts.lockedColor !== undefined}
            class="flex h-6 w-6 items-center justify-center rounded-full transition-transform hover:scale-110 disabled:cursor-not-allowed disabled:opacity-40"
            style="background-color: {paletteColor.color}"
            title={paletteColor.name}
            aria-label={paletteColor.name}
            aria-pressed={paletteColor.color === selectedColor}
            onclick={() => (newTagColor = paletteColor.color)}
          >
            {#if paletteColor.color !== selectedColor}
              <span class="bg-white-dark block h-4 w-4 rounded-full opacity-80"></span>
            {/if}
          </button>
        {/each}
      </div>
    </div>
    <button
      class="btn h-9! w-full preset-filled-primary-500"
      disabled={opts.submitDisabled}
      onclick={opts.onSubmit}
    >
      {opts.submitLabel}
    </button>
    {#if opts.lockedColor !== undefined}
      <p class="">
        The tag “{newTagName.trim()}” already exists.
      </p>
    {/if}
  </div>
{/snippet}
