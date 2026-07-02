import { describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import type { PipelineManagerApi } from '$lib/compositions/usePipelineManager.svelte'
import Tags from './Tags.svelte'

// A stub Pipeline Manager client: only `patchPipeline` is exercised by the
// single-pipeline edit paths this component triggers. The cast keeps the test
// from having to fill in the dozens of unrelated client methods.
const makeApi = () => {
  const patchPipeline = vi.fn().mockResolvedValue(undefined)
  return { api: { patchPipeline } as unknown as PipelineManagerApi, patchPipeline }
}

const renderTags = (props: { tags: string[]; knownTags?: string[] }) => {
  const { api, patchPipeline } = makeApi()
  const result = render(Tags, {
    pipelineName: 'test-pipeline',
    tags: props.tags,
    knownTags: new Set(props.knownTags ?? props.tags),
    api
  })
  return { ...result, patchPipeline }
}

describe('Tags.svelte', () => {
  describe('chips', () => {
    it('renders each assigned tag as a chip by display name, stripping the color', async () => {
      renderTags({ tags: ['dev', 'prod|ef4444'] })
      await expect.element(page.getByRole('button', { name: 'dev' })).toBeVisible()
      const prod = page.getByRole('button', { name: 'prod' })
      await expect.element(prod).toBeVisible()
      // The chip's color dot uses the encoded color (#ef4444 -> rgb).
      expect(prod.element().querySelector('span')).toHaveStyle({
        backgroundColor: 'rgb(239, 68, 68)'
      })
    })

    it('collapses the tags beyond the first two into a "+N" control', async () => {
      renderTags({ tags: ['a', 'b', 'c', 'd'] })
      await expect
        .element(page.getByRole('button', { name: 'Show all tags' }))
        .toHaveTextContent('+2')
    })

    it('shows an add-tag affordance when the pipeline has no tags', async () => {
      renderTags({ tags: [] })
      await expect.element(page.getByRole('button', { name: 'Tag' })).toBeVisible()
    })
  })

  describe('picker', () => {
    it('lists assigned and unassigned tags, the assigned one checked', async () => {
      renderTags({ tags: ['prod|ef4444'], knownTags: ['prod|ef4444', 'dev'] })
      await page.getByRole('button', { name: 'prod' }).click()

      // Both names appear in the list (selected first, then unselected).
      await expect.element(page.getByText('dev')).toBeVisible()
      const checkboxes = page.getByRole('checkbox').elements()
      expect(checkboxes).toHaveLength(2)
      const checked = checkboxes.filter((c) => (c as HTMLInputElement).checked)
      expect(checked).toHaveLength(1)
    })

    it('assigning an unselected tag patches the pipeline with it added, kept sorted', async () => {
      const { patchPipeline } = renderTags({
        tags: ['prod|ef4444'],
        knownTags: ['prod|ef4444', 'dev']
      })
      await page.getByRole('button', { name: 'prod' }).click()
      await page.getByRole('button', { name: 'dev' }).click()

      expect(patchPipeline).toHaveBeenCalledWith('test-pipeline', {
        tags: ['dev', 'prod|ef4444']
      })
    })

    it('unassigning an assigned tag patches the pipeline with it removed', async () => {
      const { patchPipeline } = renderTags({
        tags: ['dev', 'prod|ef4444'],
        knownTags: ['dev', 'prod|ef4444']
      })
      // Open via the overflow-free chip, then click the matching row to untoggle.
      await page.getByRole('button', { name: 'dev' }).first().click()
      // After opening, the same name exists as a chip and a row; the row is the
      // second match. Click it to unassign.
      await page.getByRole('button', { name: 'dev' }).last().click()

      expect(patchPipeline).toHaveBeenCalledWith('test-pipeline', {
        tags: ['prod|ef4444']
      })
    })
  })

  describe('create form validation', () => {
    it('blocks an invalid name and surfaces the reason; a valid name is color-encoded', async () => {
      const { patchPipeline } = renderTags({ tags: [] })
      await page.getByRole('button', { name: 'Tag' }).click()
      await page.getByRole('button', { name: 'Create a new tag' }).click()

      const nameInput = page.getByPlaceholder('Tag name')
      const createButton = page.getByRole('button', { name: 'Create' })

      // A character outside the allowed set is rejected up front.
      await nameInput.fill('a;b')
      await expect.element(page.getByText(/may contain only/)).toBeVisible()
      await expect.element(createButton).toBeDisabled()
      expect(patchPipeline).not.toHaveBeenCalled()

      // A valid name enables submit; the default palette color is encoded as a
      // "|rrggbb" suffix (first palette entry is red, #ef4444).
      await nameInput.fill('qa')
      await expect.element(createButton).toBeEnabled()
      await createButton.click()

      expect(patchPipeline).toHaveBeenCalledWith('test-pipeline', {
        tags: ['qa|ef4444']
      })
    })
  })
})
