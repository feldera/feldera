/**
 * Home page scroll layout.
 *
 * The page is the only scroll container: the pipelines table flows at full height and
 * the sections below it pin as one group over the table's tail, so the table reads as
 * a window the page scroll walks through while the top of the group waits at the
 * bottom of the screen.
 *
 * The assertions compare measured geometry, so the numbers hold whatever the browser's
 * scrollbar metrics are.
 */

import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-svelte'
import type { Demo, PipelineThumb } from '$lib/services/pipelineManager'

const { PLATFORM_VERSION } = vi.hoisted(() => ({ PLATFORM_VERSION: '1.0.0' }))

const thumb = (name: string): PipelineThumb =>
  ({
    name,
    description: '',
    tags: [],
    status: 'Stopped',
    storageStatus: 'Cleared',
    deploymentStatusSince: '2024-01-01T00:00:00Z',
    programStatusSince: '2024-01-01T00:00:00Z',
    deploymentError: undefined,
    platformVersion: PLATFORM_VERSION,
    programConfig: { runtime_version: null },
    deploymentResourcesStatus: 'Stopped',
    deploymentResourcesStatusSince: new Date('2024-01-01T00:00:00Z'),
    deploymentRuntimeStatusDetails: { connector_stats: { num_errors: 0 } },
    connectors: { numErrors: 0 }
  }) as unknown as PipelineThumb

// `list.names` is what the mocked composition serves; a test sets it before rendering
// to control how tall the table is.
const { list, demos } = vi.hoisted(() => ({
  // Enough rows that the table overflows any window the fixture can give it.
  list: { names: Array.from({ length: 40 }, (_, i) => `pipeline-${i}`) },
  demos: Array.from({ length: 9 }, (_, i) => ({
    name: `demo-${i}`,
    title: `Demo ${i}`,
    description: 'A demo',
    type: 'tutorial',
    program_code: ''
  }))
}))

vi.mock('$app/state', () => ({
  page: {
    url: new URL('http://localhost/'),
    data: {
      feldera: {
        version: PLATFORM_VERSION,
        revision: '0',
        edition: 'Enterprise',
        changelog: 'https://example.com/changelog',
        unstableFeatures: [],
        // Real `Permission` values, so the RBAC-gated header controls render.
        permissions: ['read:pipeline', 'write:pipeline']
      },
      auth: {
        logout: vi.fn(),
        profile: { name: 'Ada', email: 'ada@example.com' },
        userInfo: {},
        accessToken: ''
      }
    }
  }
}))
vi.mock('$app/navigation', () => ({
  goto: vi.fn(),
  invalidateAll: vi.fn(),
  preloadCode: vi.fn(() => Promise.resolve())
}))
vi.mock('$lib/services/redirectTarget', () => ({
  takeRedirectTarget: vi.fn(),
  stashRedirectTarget: vi.fn()
}))
// Dialog bodies the profile menu can open. Each fetches on import, and no test
// reaches them.
vi.mock('$lib/components/other/ApiKeyMenu.svelte', () => ({ default: () => {} }))
vi.mock('$lib/components/other/OidcTrustMenu.svelte', () => ({ default: () => {} }))
// The page and the demo tiles read the list; nothing here polls the API for it.
vi.mock('$lib/compositions/pipelines/usePipelineList.svelte', () => ({
  usePipelineList: () => ({
    get pipelines() {
      return list.names.map(thumb)
    }
  }),
  useUpdatePipelineList: () => ({
    updatePipelines: vi.fn(),
    updatePipeline: vi.fn(),
    discardPendingListRefresh: vi.fn()
  })
}))
vi.mock('$lib/compositions/useDemos.svelte', () => ({
  useDemos: () => ({
    get current() {
      return demos as unknown as Demo[]
    }
  }),
  loadDemos: vi.fn()
}))

// Imported AFTER vi.mock so the mocks take effect.
import { pinnedPeekHeightPixels } from '$lib/components/layout/PinnedSections.svelte'
import HomePage from './+page.svelte'

const SCROLL_AREA_HEIGHT = 800
// What the pinned group shows at rest; the table's window gets the rest of the screen.
const PEEK = pinnedPeekHeightPixels
const ALL_PIPELINES = list.names

/** The wrapper around the demos, which is the pinned group's only section. */
const demosSection = (container: HTMLElement) =>
  [...container.querySelectorAll<HTMLElement>('[data-testid=box-pinned-sections] > div')].find(
    (section) => section.textContent?.includes('Explore use cases and tutorials')
  )!

/** The demos' collapse toggle, which a click reaches only once the section is up. */
const demosHeader = (section: HTMLElement) =>
  section.querySelector<HTMLElement>('[role=presentation]')!

const click = (element: HTMLElement) => element.click()

/** Mounts the page in a container of known size and returns the scroll geometry. */
const renderHome = async ({ width = 1200 }: { width?: number } = {}) => {
  // The welcome banner is a one-time greeting at the top of the page; dismissed is the
  // state the page spends its life in.
  localStorage.setItem('home/welcomed', 'true')
  const { container } = render(HomePage)
  container.style.width = `${width}px`
  container.style.height = `${SCROLL_AREA_HEIGHT}px`

  const scrollArea = container.querySelector<HTMLElement>('[data-testid=box-home-scroll-area]')!
  const pipelines = container.querySelector<HTMLElement>('[data-testid=box-pipelines-section]')!
  const pinnedGroup = container.querySelector<HTMLElement>('[data-testid=box-pinned-sections]')!
  // The pin offset comes from the measured group, so it settles a frame after the
  // container is sized and the group laid out.
  await expect.poll(() => scrollArea.clientHeight).toBe(SCROLL_AREA_HEIGHT)
  await new Promise((settled) => requestAnimationFrame(() => requestAnimationFrame(settled)))
  return { container, scrollArea, pipelines, pinnedGroup }
}

describe('/ (home) scroll layout', () => {
  beforeEach(() => {
    list.names = ALL_PIPELINES
  })

  it('leaves the page as the only scroll container', async () => {
    const { scrollArea, pipelines } = await renderHome()

    // The page grows with the table, which gives the single scrollbar enough range to
    // walk through the table.
    expect(scrollArea.scrollHeight).toBeGreaterThan(scrollArea.clientHeight)
    // Nothing nested scrolls: a second scroll container would mean a second scroll
    // position, and a wheel gesture over the table would have to reach the table's end
    // before the page moved at all.
    const nestedScrollers = [...scrollArea.querySelectorAll<HTMLElement>('*')].filter((el) => {
      const overflowY = getComputedStyle(el).overflowY
      return (
        (overflowY === 'auto' || overflowY === 'scroll') && el.scrollHeight > el.clientHeight + 1
      )
    })
    expect(nestedScrollers).toEqual([])
    expect(getComputedStyle(pipelines).overflowY).toBe('visible')
  })

  it('leaves the table a window with the section group pinned below it', async () => {
    const { scrollArea, pinnedGroup } = await renderHome()

    const visible = scrollArea.getBoundingClientRect()
    const pinned = pinnedGroup.getBoundingClientRect()
    expect(scrollArea.scrollTop).toBe(0)
    // The group starts where the table's window ends...
    expect(pinned.top - visible.top).toBeCloseTo(SCROLL_AREA_HEIGHT - PEEK, 0)
    // ...and reaches past the bottom of the screen, so only the peek shows.
    expect(pinned.bottom).toBeGreaterThan(visible.bottom)
    // Opaque, or the rows the group is pinned over would show through.
    expect(getComputedStyle(pinnedGroup).backgroundColor).not.toBe('rgba(0, 0, 0, 0)')
  })

  it('scrolls the table under the pinned group', async () => {
    const { scrollArea, pinnedGroup } = await renderHome()

    const rowBefore = scrollArea.querySelector('tbody tr')!.getBoundingClientRect().top
    const pinnedBefore = pinnedGroup.getBoundingClientRect().top
    scrollArea.scrollTop = 200
    expect(scrollArea.scrollTop).toBe(200)

    // The rows move with the page while the group stays put: one scroll position
    // drives both the table window and the page.
    expect(scrollArea.querySelector('tbody tr')!.getBoundingClientRect().top).toBeCloseTo(
      rowBefore - 200,
      0
    )
    expect(pinnedGroup.getBoundingClientRect().top).toBeCloseTo(pinnedBefore, 0)
  })

  it('releases the group once the table has been scrolled through', async () => {
    const { scrollArea, pipelines, pinnedGroup } = await renderHome()
    const pinnedTop = pinnedGroup.getBoundingClientRect().top

    scrollArea.scrollTop = scrollArea.scrollHeight
    const visible = scrollArea.getBoundingClientRect()
    const released = pinnedGroup.getBoundingClientRect()
    // The group has moved up out of the pinned position and clears the bottom of the
    // screen, so its tail is reachable.
    expect(released.top).toBeLessThan(pinnedTop)
    expect(released.bottom).toBeLessThanOrEqual(visible.bottom + 1)
    // The table is clear of the group: every row the peek covered is scrolled
    // through.
    expect(pipelines.getBoundingClientRect().bottom).toBeLessThanOrEqual(released.top + 1)
  })

  it('does not pin the group when the pipelines fit on screen', async () => {
    list.names = ALL_PIPELINES.slice(0, 3)
    const { scrollArea, pipelines, pinnedGroup } = await renderHome()

    // Three rows leave the group room above the pin line, so the group keeps its
    // place in the flow, `gap-8` below the table, without floating over the gap.
    const gap = pinnedGroup.getBoundingClientRect().top - pipelines.getBoundingClientRect().bottom
    expect(gap).toBeCloseTo(32, 0)
    expect(scrollArea.scrollTop).toBe(0)
  })

  it('pins the whole group when a section is added above the demos', async () => {
    const { scrollArea, pinnedGroup } = await renderHome()
    const offsetBefore = pinnedGroup.style.bottom

    // Stands in for a section added at the top of the group: the peek keeps its
    // height and shows this section, which pushes the demos below the screen edge.
    const added = document.createElement('div')
    added.style.height = '300px'
    added.textContent = 'ADDED SECTION'
    pinnedGroup.prepend(added)
    await expect.poll(() => pinnedGroup.style.bottom).not.toBe(offsetBefore)

    const visible = scrollArea.getBoundingClientRect()
    expect(pinnedGroup.getBoundingClientRect().top - visible.top).toBeCloseTo(
      SCROLL_AREA_HEIGHT - PEEK,
      0
    )
    // The added section owns the peek; the demos start below the screen edge.
    expect(added.getBoundingClientRect().top).toBeCloseTo(visible.bottom - PEEK, 0)
    const demos = pinnedGroup.lastElementChild!.getBoundingClientRect()
    expect(demos.top).toBeGreaterThanOrEqual(visible.bottom - 1)
  })

  it('brings the demos section up when its peek is clicked', async () => {
    const { container, scrollArea } = await renderHome()
    const section = demosSection(container)
    const visible = scrollArea.getBoundingClientRect()

    click(demosHeader(section))

    // The section is taller than half the screen, so it comes up far enough to show
    // all of it and no further.
    await expect
      .poll(() => Math.round(section.getBoundingClientRect().bottom))
      .toBe(Math.round(visible.bottom))
    const top = section.getBoundingClientRect().top - visible.top
    expect(top).toBeGreaterThan(0)
    expect(top).toBeLessThanOrEqual(SCROLL_AREA_HEIGHT / 2)
    // The click went to the section, not to the collapse toggle under the pointer, so
    // the demos are still on show.
    expect(section.textContent).toContain('Demo 0')
  })

  it('stops the top of a short section at the middle of the screen', async () => {
    const { container, scrollArea, pinnedGroup } = await renderHome()
    const section = demosSection(container)
    const offsetBefore = pinnedGroup.style.bottom
    // Short enough to fit below the middle, with room left underneath to scroll
    // through: at the very end of the page there would be nothing to scroll into.
    section.style.height = '200px'
    const below = document.createElement('div')
    below.style.height = '400px'
    pinnedGroup.after(below)
    await expect.poll(() => pinnedGroup.style.bottom).not.toBe(offsetBefore)

    click(demosHeader(section))

    const visible = scrollArea.getBoundingClientRect()
    await expect
      .poll(() => Math.round(section.getBoundingClientRect().top - visible.top))
      .toBe(SCROLL_AREA_HEIGHT / 2)
  })

  it('lets a click through once the section is in view', async () => {
    const { container, scrollArea } = await renderHome()
    const section = demosSection(container)
    const visible = scrollArea.getBoundingClientRect()

    click(demosHeader(section))
    await expect
      .poll(() => section.getBoundingClientRect().bottom)
      .toBeLessThanOrEqual(visible.bottom + 1)

    // The section is up, so this click reaches the collapse toggle.
    click(demosHeader(section))

    await expect.poll(() => section.textContent).not.toContain('Demo 0')
  })
})
