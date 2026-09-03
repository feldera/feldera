/**
 * Home page scroll layout.
 *
 * The page is the only scroll container. The pipelines table is laid out at full
 * height, and `PinnedSections` holds the sections below it at the bottom of the screen
 * until the table has scrolled past.
 *
 * Needs the browser project, because the assertions are layout measurements. They
 * compare measured geometry rather than pixel constants, so they hold whatever the
 * browser's scrollbar metrics are.
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

// The page reads `pipelines` on every reactive pass. Returning the same array for the
// same names keeps the table from rebuilding its rows on each read, which leaves fewer
// of `TableHandler`'s deferred scroll writes pending at teardown.
let cachedThumbs: { names: string[]; thumbs: PipelineThumb[] } | undefined
const thumbsFor = (names: string[]) => {
  if (cachedThumbs?.names !== names) {
    cachedThumbs = { names, thumbs: names.map(thumb) }
  }
  return cachedThumbs.thumbs
}

// The mocked composition below reads `list.names`. A test sets it before rendering to
// control how tall the table is.
const { list, demos } = vi.hoisted(() => ({
  // Enough rows to overflow the fixture's height.
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
// Dialog bodies the profile menu can open. Each one fetches on import, and no test
// opens them.
vi.mock('$lib/components/other/ApiKeyMenu.svelte', () => ({ default: () => {} }))
vi.mock('$lib/components/other/OidcTrustMenu.svelte', () => ({ default: () => {} }))
// The page and the demo tiles read this list. Nothing here polls the API.
vi.mock('$lib/compositions/pipelines/usePipelineList.svelte', () => ({
  usePipelineList: () => ({
    get pipelines() {
      return thumbsFor(list.names)
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
// How much of the wrapper shows while it is pinned.
const PEEK = pinnedPeekHeightPixels
const ALL_PIPELINES = list.names

/** The demos section, which is the wrapper's only child. */
const demosSection = (container: HTMLElement) =>
  [...container.querySelectorAll<HTMLElement>('[data-testid=box-pinned-sections] > div')].find(
    (section) => section.textContent?.includes('Explore use cases and tutorials')
  )!

/**
 * The demos' collapse toggle. A click reaches it only once the section is fully on
 * screen.
 */
const demosHeader = (section: HTMLElement) =>
  section.querySelector<HTMLElement>('[role=presentation]')!

/**
 * A pointer click.
 *
 * `HTMLElement.click()` reports `detail` 0, which `PinnedSections` treats as keyboard
 * activation. Playwright's click would report 1 but scrolls the target into view
 * first, which undoes the state under test.
 */
const click = (element: HTMLElement) =>
  element.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, detail: 1 }))

/** Mounts the page in a container of known size and returns the scroll geometry. */
const renderHome = async ({ width = 1200 }: { width?: number } = {}) => {
  // The welcome banner greets the user once. Dismissed is the state the page is in for
  // every visit after the first.
  localStorage.setItem('home/welcomed', 'true')
  const { container } = render(HomePage)
  container.style.width = `${width}px`
  container.style.height = `${SCROLL_AREA_HEIGHT}px`

  const scrollArea = container.querySelector<HTMLElement>('[data-testid=box-home-scroll-area]')!
  const pipelines = container.querySelector<HTMLElement>('[data-testid=box-pipelines-section]')!
  const wrapper = container.querySelector<HTMLElement>('[data-testid=box-pinned-sections]')!
  // `stickyBottom` comes from the measured height, so it is set a frame after the
  // container is sized.
  await expect.poll(() => scrollArea.clientHeight).toBe(SCROLL_AREA_HEIGHT)
  await new Promise((settled) => requestAnimationFrame(() => requestAnimationFrame(settled)))
  return { container, scrollArea, pipelines, wrapper }
}

describe('/ (home) scroll layout', () => {
  beforeEach(() => {
    list.names = ALL_PIPELINES
    // `renderHome` and the collapse toggle both write to localStorage, so without this
    // a test appended after one that toggles would start with the demos collapsed.
    localStorage.clear()
  })

  it('leaves the page as the only scroll container', async () => {
    const { scrollArea, pipelines } = await renderHome()

    // The page is taller than the screen, so its one scrollbar covers the whole table.
    expect(scrollArea.scrollHeight).toBeGreaterThan(scrollArea.clientHeight)
    // A nested scroll container would add a second scroll position, and a wheel
    // gesture over the table would move the table to its end before the page moved at
    // all.
    const nestedScrollers = [...scrollArea.querySelectorAll<HTMLElement>('*')].filter((el) => {
      const overflowY = getComputedStyle(el).overflowY
      return (
        (overflowY === 'auto' || overflowY === 'scroll') && el.scrollHeight > el.clientHeight + 1
      )
    })
    expect(nestedScrollers).toEqual([])
    expect(getComputedStyle(pipelines).overflowY).toBe('visible')
  })

  it('pins the wrapper below the visible part of the table', async () => {
    const { scrollArea, wrapper } = await renderHome()

    const visible = scrollArea.getBoundingClientRect()
    const pinned = wrapper.getBoundingClientRect()
    expect(scrollArea.scrollTop).toBe(0)
    // The wrapper's top is one peek above the bottom of the screen...
    expect(pinned.top - visible.top).toBeCloseTo(SCROLL_AREA_HEIGHT - PEEK, 0)
    // ...and its bottom is past the screen edge, so only the peek shows.
    expect(pinned.bottom).toBeGreaterThan(visible.bottom)
    // Opaque, or the rows behind the wrapper would show through.
    expect(getComputedStyle(wrapper).backgroundColor).not.toBe('rgba(0, 0, 0, 0)')
  })

  it('scrolls the table under the pinned wrapper', async () => {
    const { scrollArea, wrapper } = await renderHome()

    const rowBefore = scrollArea.querySelector('tbody tr')!.getBoundingClientRect().top
    const pinnedBefore = wrapper.getBoundingClientRect().top
    scrollArea.scrollTop = 200
    expect(scrollArea.scrollTop).toBe(200)

    // One scroll position moves the rows and leaves the wrapper where it is.
    expect(scrollArea.querySelector('tbody tr')!.getBoundingClientRect().top).toBeCloseTo(
      rowBefore - 200,
      0
    )
    expect(wrapper.getBoundingClientRect().top).toBeCloseTo(pinnedBefore, 0)
  })

  it('unpins the wrapper at the end of the page', async () => {
    const { scrollArea, pipelines, wrapper } = await renderHome()
    const pinnedTop = wrapper.getBoundingClientRect().top

    scrollArea.scrollTop = scrollArea.scrollHeight
    const visible = scrollArea.getBoundingClientRect()
    const released = wrapper.getBoundingClientRect()
    // The wrapper has moved up out of the pinned position, and its bottom is now on
    // screen.
    expect(released.top).toBeLessThan(pinnedTop)
    expect(released.bottom).toBeLessThanOrEqual(visible.bottom + 1)
    // No part of the table is behind the wrapper.
    expect(pipelines.getBoundingClientRect().bottom).toBeLessThanOrEqual(released.top + 1)
  })

  it('does not pin the wrapper when the pipelines fit on screen', async () => {
    list.names = ALL_PIPELINES.slice(0, 3)
    const { scrollArea, pipelines, wrapper } = await renderHome()

    // Three rows leave the wrapper above the bottom of the screen, so it stays in the
    // flow, `gap-8` below the table.
    const gap = wrapper.getBoundingClientRect().top - pipelines.getBoundingClientRect().bottom
    expect(gap).toBeCloseTo(32, 0)
    expect(scrollArea.scrollTop).toBe(0)
  })

  it('pins the whole wrapper when a section is added above the demos', async () => {
    const { scrollArea, wrapper } = await renderHome()
    const offsetBefore = wrapper.style.bottom

    // Stands in for a section added at the top of the wrapper. The peek keeps its
    // height, so this section fills it and the demos move below the screen edge.
    const added = document.createElement('div')
    added.style.height = '300px'
    added.textContent = 'ADDED SECTION'
    wrapper.prepend(added)
    await expect.poll(() => wrapper.style.bottom).not.toBe(offsetBefore)

    const visible = scrollArea.getBoundingClientRect()
    expect(wrapper.getBoundingClientRect().top - visible.top).toBeCloseTo(
      SCROLL_AREA_HEIGHT - PEEK,
      0
    )
    // The added section fills the peek, and the demos start below the screen edge.
    expect(added.getBoundingClientRect().top).toBeCloseTo(visible.bottom - PEEK, 0)
    const demos = wrapper.lastElementChild!.getBoundingClientRect()
    expect(demos.top).toBeGreaterThanOrEqual(visible.bottom - 1)
  })

  it('brings the demos section into view when its peek is clicked', async () => {
    const { container, scrollArea, wrapper } = await renderHome()
    const section = demosSection(container)
    const offsetBefore = wrapper.style.bottom
    // Set here rather than inherited from the demo tiles: how many tiles render depends
    // on a media query against the real viewport, not on the container this test sizes.
    section.style.height = `${SCROLL_AREA_HEIGHT * 0.75}px`
    await expect.poll(() => wrapper.style.bottom).not.toBe(offsetBefore)
    const visible = scrollArea.getBoundingClientRect()

    click(demosHeader(section))

    // The section is taller than half the screen, so it stops where all of it shows
    // rather than at the middle.
    await expect
      .poll(() => Math.round(section.getBoundingClientRect().bottom))
      .toBe(Math.round(visible.bottom))
    const top = section.getBoundingClientRect().top - visible.top
    expect(top).toBeGreaterThan(0)
    expect(top).toBeLessThanOrEqual(SCROLL_AREA_HEIGHT / 2)
    // The click scrolled the section instead of reaching the collapse toggle under the
    // pointer, so the demos are still expanded.
    expect(section.textContent).toContain('Demo 0')
  })

  it('stops the top of a short section at the middle of the screen', async () => {
    const { container, scrollArea, wrapper } = await renderHome()
    const section = demosSection(container)
    const offsetBefore = wrapper.style.bottom
    // Short enough to fit in the lower half of the screen, with a spacer below it so
    // there is somewhere left to scroll.
    section.style.height = '200px'
    const below = document.createElement('div')
    below.style.height = '400px'
    wrapper.after(below)
    await expect.poll(() => wrapper.style.bottom).not.toBe(offsetBefore)

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

    // All of the section shows now, so this click reaches the collapse toggle.
    click(demosHeader(section))

    await expect.poll(() => section.textContent).not.toContain('Demo 0')
  })
})
