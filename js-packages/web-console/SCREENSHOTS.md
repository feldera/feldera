# Auto-generated UI screenshots for the docs

Plan for generating `docs.feldera.com` UI screenshots, and for regenerating them when the UI changes.
The primary path is vitest browser mode on its Playwright provider. A secondary Playwright e2e path
covers whole-app pages.

## Motivation

The docs carry 64 images. Roughly 20 are UI screenshots; the rest are architecture diagrams and are
out of scope. The 12 images in `docs.feldera.com/docs/tour/` were last touched on 2025-06-05 and
were captured by hand at 1245x813, non-retina. Nothing ties them to the current UI, so they drift
silently until a reader notices.

Goals:

| Goal | Meaning |
| --- | --- |
| Reproducible | Two runs on the same commit produce byte-identical PNGs |
| Automatic | One command regenerates every screenshot |
| Drift-detecting | CI fails when a UI change invalidates a committed screenshot |
| Reviewable | Image diffs appear in the PR that caused them |

## How vitest browser mode works here

Worth stating precisely, because most of the traps below follow from the architecture.

```
node process                          chromium (playwright)
+----------------------------+        +-----------------------------------+
| vitest                     |        | orchestrator page                 |
|  vite dev server           |<------>|   #vitest-tester  <iframe>        |
|  @vitest/browser-playwright|   ws   |     tester page (one per file)    |
|    browser.launch()        |        |       your test code              |
|    context.newPage()       |        |       your component              |
+----------------------------+        +-----------------------------------+
```

Vitest serves the app's module graph from a Vite dev server, then the Playwright provider launches
a real Chromium, opens a `BrowserContext` from `contextOptions`, and navigates a `Page` to an
orchestrator document. Each test file is loaded into an iframe inside that page, sized by
`instances[].viewport`.

Test code runs inside the iframe, in the same realm as the component. `document` and `window` are
the component's own. Anything requiring real browser control (click, hover, viewport, screenshot) is
sent over a websocket to the node-side provider, which performs it through Playwright's API against
the frame.

Consequences that matter:

| Property | Consequence |
| --- | --- |
| Real Chromium, real CSS engine | Screenshots are production-faithful |
| `page` is a facade, not a Playwright `Page` | Locators become selectors, re-resolved node-side |
| Component harness, not the SvelteKit runtime | No router, no layouts, no load functions |
| Content lives in an iframe | `fullPage` is unavailable, and the iframe can be scaled |

Determinism is the reason to prefer this path. Component state comes from props and mocks, so there
is no server clock, no build version, and no instance state to normalise. The nondeterminism that
would otherwise have to be engineered away simply does not exist.

## Capture geometry and resolution

Both modes default to output unfit for docs, for different reasons. Neither default is usable
as-is, so both need explicit configuration.

| | vitest browser | Playwright e2e |
| --- | --- | --- |
| Content renders in | iframe inside an orchestrator page | the top-level page |
| Logical size knob | `instances[].viewport` | `use.viewport` |
| Real window size knob | `contextOptions.viewport` | same as `use.viewport` |
| Retina knob | `contextOptions.deviceScaleFactor` | `use.deviceScaleFactor` |
| Default when unset | 414x896, then downscaled | 1280x720 at dsf 1 |
| Downscaling trap | yes, see below | none |
| `fullPage` | unavailable | works |
| Element clip | `screenshot({ element })` | `locator.screenshot()` |
| Output path base | the test file's directory | the process working directory |

### vitest: `page.screenshot()` is a body locator screenshot

With no `element` argument it calls `locator('body').screenshot()`, not a page screenshot
(`@vitest/browser-playwright/dist/index.js:535`). It therefore clips to the body box, which is also
why `fullPage` does nothing. Pass `element` for a clipped capture.

### vitest: the iframe scaling trap

`test.browser.ui` defaults to `true` outside CI. It places the tester iframe in a `splitpanes__pane`
and applies `transform: scale(...)` on `#tester-ui` to fit. Playwright captures rendered pixels, so
the output is `viewport * scale`. Enlarging the viewport makes the image smaller, because a larger
iframe is scaled down harder. Measured on a 414x896 tester viewport:

| Configuration | Window | Scale | Capture |
| --- | --- | --- | --- |
| Repository default | 800x600 | 0.379 | 158x340 |
| `--window-size=1920,1200` | 1920x1200 | 0.977 | 405x876 |
| plus `browser.ui: false` | 1280x720 | 0.900 | 1152x720 |
| plus context viewport 1600x1000, `deviceScaleFactor: 2` | 1600x1000 | 1.000 | 2560x1600 |

The rule is `scale = min(1, windowW / viewportW, windowH / viewportH)`. The context viewport must be
at least as large as the tester viewport.

Two corollaries. `page.viewport()` at runtime resizes the iframe but not its container, so it
changes the scale rather than the capture size; configure the viewport statically instead. And set
the window size through `contextOptions.viewport`, not the `--window-size` launch argument: the
launch argument did not survive turning `browser.ui` off during testing, whereas an explicit context
viewport was honoured in every configuration tried.

### e2e: the defaults are worse than the images being replaced

`playwright.config.ts` sets only `baseURL`. Playwright then defaults to a 1280x720 viewport at
`deviceScaleFactor` 1, so a docs capture would land at 1280x720 non-retina, no better than the
hand-made 1245x813 images it is meant to replace. Measured:

| Context options | Viewport | Viewport capture | `fullPage` capture |
| --- | --- | --- | --- |
| none (Playwright defaults) | 1280x720 at dpr 1 | 1280x720 | 1280x2029, 12KB |
| `1440x900`, `deviceScaleFactor: 2` | 1440x900 at dpr 2 | 2880x1800 | 2880x4058, 44KB |

Docs shots therefore need their own Playwright project rather than the inherited `use` block:

```ts
// playwright.config.ts
projects: [
  { name: 'e2e', testMatch: /(.+\.)?e2e\.[jt]s/ },
  {
    name: 'docs-screenshots',
    testMatch: /\.shot\.[jt]s/,
    use: { viewport: { width: 1440, height: 900 }, deviceScaleFactor: 2 }
  }
]
```

`fullPage` is available here and is the right choice for a long page, but note that it multiplies
height by the device scale factor too. Prefer a viewport or element clip unless the shot genuinely
needs the whole scroll length.

### Output paths

In vitest, relative `path` values resolve against the test file's directory, and absolute paths
outside the project root are permitted: writing to
`/workspaces/feldera/docs.feldera.com/docs/tour/x.png` from a browser test succeeds. Docs output can
be written in place, with no copy step. Use absolute paths built from a shared constant rather than
counting `../` segments from a spec file.

Keep this separate from `toMatchScreenshot`, which the `browser.expect.toMatchScreenshot.resolveScreenshotPath`
hook already routes to `playwright-snapshots/component/`. That mechanism is for visual regression
baselines; docs images are a different artifact with a different lifecycle.

### Debugging captures

Four things that cost time during investigation and are not obvious from the APIs.

`page.screenshot({ base64: true, save: false })` returns a bare string in vitest 4.1.10, not the
`{ path, base64 }` object the types suggest. With `save: true` it returns the resolved path string.

`console.log` from a passing browser test does not reliably reach the terminal. To inspect a value
mid-investigation, assert it against a sentinel so the diff prints it, or write it to a file from
the node side.

A failing browser test auto-writes a failure screenshot to `__screenshots__/` beside the spec. Those
are untracked build output and should be cleaned up before checking `git status`.

To read PNG dimensions without an image library, the IHDR chunk puts width at byte offset 16 and
height at byte 20. On a Node `Buffer`, use `buf.readUInt32BE(16)`; `buf.buffer` includes the pool
byte offset and will decode garbage.

Standalone Playwright scripts must run from inside `js-packages/web-console`. Bun's flat node_modules
layout means `playwright` does not resolve from an arbitrary directory such as `/tmp`.

## Styling the harness

The harness renders components with browser defaults, not the product theme. This was the single
largest fidelity gap and is now fixed.

### Why it happened

Two attributes carry the entire design system, and both are set outside every component:

| Attribute | Set by | Drives |
| --- | --- | --- |
| `data-theme="feldera-modern-theme"` | `src/app.html:2` | Every feldera-theme custom property |
| `light` / `dark` class | `src/routes/+layout.svelte:52` via `HtmlAttr` | The `dark` custom variant in `src/routes/layout.css:20` |

`src/routes/layout.css` is already imported by the setup file, so Tailwind, Skeleton, the
feldera-theme sheet, DM Sans, DM Mono, Font Awesome brands, and the `fd`/`gc` icon webfonts are all
present. The problem is not that the CSS is absent; it is that the CSS never activates.

`js-packages/feldera-theme/feldera-modern.css:1` scopes all its tokens under
`[data-theme="feldera-modern-theme"]`, including `--base-font-family: DM Sans Variable`. Skeleton's
base layer applies them with `body { font-family: var(--base-font-family); color: var(--base-font-color); ... }`.
With no `data-theme` on the root, those custom properties resolve to nothing and the cascade falls
back to Tailwind's preflight.

Measured on the same component, before and after:

| Property | Without the root attributes | With them |
| --- | --- | --- |
| `body` font-family | `ui-sans-serif, system-ui, ...` | `"DM Sans Variable"` |
| `--base-font-family` | unset | `DM Sans Variable` |
| `body` color | `rgb(0, 0, 0)` | `oklch(0.3677 0 none)` |
| `body` background | `oklch(0.985 0 0)` | `rgb(255, 255, 255)` |
| `document.fonts.check('16px "DM Sans Variable"')` | `false` | `true` |

Visually, a danger dialog rendered with grey buttons and system text before, and with the correct
red preset and DM Sans after.

### The fix

`src/lib/testAppShell.ts` replicates the app's document-root state, and
`src/lib/vitest-browser-setup.ts` calls it. Applying it in the shared setup file is deliberate:
every existing browser test becomes more faithful to production, at no cost.

```ts
export const applyAppShell = (mode: ColorScheme = 'light') => {
  document.documentElement.dataset.theme = 'feldera-modern-theme'
  document.documentElement.classList.remove('light', 'dark')
  document.documentElement.classList.add(mode)
  document.body.classList.add('h-screen')
}
```

Verified against the whole `client` project: 222 tests pass with the theme applied.

Fonts load asynchronously, because `src/assets/fonts/dm-sans.css` uses `font-display: block`. A
screenshot taken before they resolve captures fallback glyphs. `waitForFonts()` in the same module
awaits `document.fonts.load(...)` then `document.fonts.ready`, and every capture must await it.

### Remaining harness gaps

`src/app.html:10` wraps the app in `<div class="h-screen">`. `vitest-browser-svelte` appends its own
container to `body` without that class, so a component relying on `h-full` through its ancestors
measures zero height. Pass a prepared container to `render()` for those cases rather than changing
the default, which would alter layout for existing tests.

Dark-mode captures are nearly free: call `applyAppShell('dark')` in the shot, emit `foo-dark.png`
alongside `foo.png`.

## Do not change the shared `client` project

The geometry settings above must live in a separate vitest project. Applying a 1280x800 viewport to
`client` breaks `src/lib/components/layout/OverlayDrawer.svelte.spec.ts:30`, which asserts
`window.innerWidth < 700` to guarantee its narrow-screen precondition. That test is correct; the
screenshot project simply has different requirements. A `deviceScaleFactor` change would likewise
rescale every future `toMatchScreenshot` baseline.

```ts
// vite.config.ts, alongside the existing projects
{
  extends: './vite.config.ts',
  test: {
    name: 'screenshots',
    include: ['screenshots/**/*.shot.ts'],
    setupFiles: ['src/lib/vitest-browser-setup.ts'],
    browser: {
      enabled: true,
      ui: false,
      provider: playwright({
        contextOptions: { viewport: { width: 1600, height: 1000 }, deviceScaleFactor: 2 }
      }),
      instances: [{ browser: 'chromium', headless: true, viewport: { width: 1280, height: 800 } }]
    }
  }
}
```

The screenshot project is excluded from `test-unit`, so ordinary test runs never pay for it.

## Secondary path: Playwright e2e for whole pages

Vitest browser mode has no SvelteKit runtime, so a route with layouts, a router, and load functions
cannot be mounted without mocking `$app/*` and hand-feeding `data`. That is worth doing for a
composed view, but not for the application shell.

Shots that must show the real app, such as `docs/tour/home.png` with its navigation bar and pipeline
table, use the existing `playwright.config.ts` e2e setup against a running pipeline-manager, with
the `docs-screenshots` project added above for resolution. A live capture during investigation
produced a correct 2880x1800 retina image of the home page, with DM Sans loading correctly without
any harness intervention: this path renders the real `src/app.html`, so it never had the theming
problem described below.

Note that `workers: 1` in the existing config is deliberate, because compilation is a shared global
resource. Docs shots inherit that serialization, which is another reason to keep this path small.

| Shot kind | Path | Determinism source |
| --- | --- | --- |
| Component, dialog, detail | vitest browser | props and mocks |
| Composed view | vitest browser with mocked `$app/*` | props and mocks |
| Whole page, navigation, routing | Playwright e2e | seeded instance state |

### Determinism for the e2e path

Only this path needs instance control, and only for the handful of whole-page shots.

Reset and seed through the API. `src/lib/services/pipelineManager.ts` already exports `getPipelines`,
`putPipeline`, `deletePipeline`, and `postPipelineAction`; `src/lib/services/testPipelineHelpers.ts`
adds `configureTestClient`, `waitForCompilation`, `startPipelineAndWaitForRunning`, `cleanupPipeline`,
and `warmCompilationCache`. `tests/pipelineSearch.e2e.ts` is the working precedent.

```ts
export async function resetInstance() {
  assertWipeAllowed()
  const existing = await getPipelines()
  await Promise.all(existing.map((p) => cleanupPipeline(p.name)))
}
```

`resetInstance` deletes every pipeline on the target, so it must refuse unless
`FELDERA_SCREENSHOT_ALLOW_WIPE=1` is set and the origin is localhost or explicitly allowlisted, and
it must print what it is about to delete. Fixture pipelines are chosen to read well in docs, with
names such as `fraud-detection` and SQL short enough to fit an editor screenshot.

Seeding cannot fix server-derived values. The live capture contained `5h 1m ago`, `14d 5h 45m ago`,
and `0.332.0`. Freeze the clock with `page.clock.install()` and rewrite `deployed_on` and
`status_since` via a `page.route` interceptor so relative times render as stable strings. On version
strings, leave them real and accept one regeneration per release, since release commits already
touch the tree.

Suppress transient chrome through `addInitScript`: the promotional banner is the `home/welcomed`
localStorage key (`src/routes/(system)/(authenticated)/(authorized)/+page.svelte:53`), the theme is
`darkMode` (`src/lib/compositions/useDarkMode.svelte.ts:4`). Route-abort PostHog and Product Fruits,
which inject overlays and add nondeterministic timing. Do not wait on `networkidle`; the console
polls pipeline status and never stays idle.

## Layout

```
js-packages/web-console/screenshots/
  *.shot.ts            one file per docs area, using render() + page.screenshot()
  manifest.ts          output paths and viewport overrides
  README.md
js-packages/web-console/tests/
  docs.e2e.ts          whole-page shots, gated on PLAYWRIGHT_APP_ORIGIN
```

Images are written directly into `docs.feldera.com/docs/**`, next to the Markdown that references
them. The docs use relative references such as `![Feldera home screen](home.png)`, so writing in
place keeps those working and puts image review beside prose review.

## Commands

| Script | Behaviour |
| --- | --- |
| `screenshots:update` | Capture and write into `docs.feldera.com` |
| `screenshots:check` | Capture to a temp directory, compare against committed images, exit non-zero on drift |
| `screenshots:update -- <id>` | Regenerate one shot |

`screenshots:check` uses the pixel-ratio tolerance already configured for e2e in
`playwright.config.ts` (`maxDiffPixelRatio: 0.01`), which absorbs font-hinting noise without hiding
layout changes.

## CI

Add `.github/workflows/screenshots.yml`, modelled on `.github/workflows/test-web-console-e2e.yml`,
which already provides the `mcr.microsoft.com/playwright:v1.58.2-noble` container and a
`pipeline-manager` service with `AUTH_PROVIDER: none`. Pinning that image also pins the font stack,
which matters because a different font package changes every pixel.

The vitest path needs only the container. The e2e path additionally needs the service.

| Trigger | Job | On failure |
| --- | --- | --- |
| PR touching `js-packages/web-console/**` | `screenshots:check` | Fail, upload diffs as an artifact, comment with the changed shot ids |
| `workflow_dispatch`, or a `regen-screenshots` label | `screenshots:update` | Commit updated PNGs to the PR branch |

The label path makes regeneration one click: CI names the broken shots, the author adds the label,
CI pushes new images into the same PR where a reviewer sees them next to the code that changed them.

Do not regenerate automatically on every push. Screenshot churn in unrelated PRs is how image
pipelines become noise that reviewers learn to skip.

## Rollout

| Phase | Work | Result |
| --- | --- | --- |
| 1 | `screenshots` vitest project, one component shot | Byte-identical output across two local runs |
| 2 | Migrate the detail shots under `docs/pipelines/` to component captures | Hand-cropping retired |
| 3 | `screenshots:check` in CI on web-console PRs | Drift becomes visible |
| 4 | The `regen-screenshots` label path | Drift becomes one click to fix |
| 5 | e2e path plus fixtures for the `docs/tour/` whole-page shots | The staleest set is generated |

Phase 1 carries the real risk. Run `screenshots:update` twice on an unchanged tree and confirm
`git diff` is empty before the set grows. Phase 5 is last because it is the only phase needing
instance state, and the safety guard around `resetInstance` deserves its own review.

## Open questions

Image size. Retina output is several times heavier than what the docs carry today. A measured
2880x1800 capture of the home page came to 244KB, against 1245x813 for the current hand-made images.
Twenty shots add roughly 5MB, and each regeneration adds another copy to git history. `fullPage`
shots are worse, since the device scale factor multiplies height as well. Add `oxipng` to the
capture step in phase 1, prefer viewport and element clips over `fullPage`, and revisit if history
growth becomes a problem.

Authenticated and enterprise views. The CI instance runs `AUTH_PROVIDER: none`, so any shot showing
the tenant selector cannot be generated there. Vitest browser mode sidesteps this for component
shots, since auth state is just a prop. Defer until a whole-page shot needs it.
