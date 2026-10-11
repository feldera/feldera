# Docs screenshots

These Playwright scripts capture the web console screenshots into `docs.feldera.com/docs`. A script sets
up one state of the UI, then compares it with the image in the docs. It writes a new image only when
more than 0.5% of the pixels changed (`maxDiffPixelRatio` in `../playwright-screenshots.config.ts`).

## Two kinds of shots

| Project          | Files                   | Backend                                | Where the state comes from                                               |
| ---------------- | ----------------------- | -------------------------------------- | ------------------------------------------------------------------------ |
| `profile-viewer` | `*.shot.ts`             | None. Every `/v0/` request is refused. | `fixtures/fraud-detection-support-bundle.zip`                            |
| `web-console`    | `*.web-console.shot.ts` | A real pipeline manager                | Pipelines that the test creates under its own names and deletes after it |

## Commands

Run them in `js-packages/web-console`:

| Command                                             | What it does                                                          |
| --------------------------------------------------- | --------------------------------------------------------------------- |
| `bun run docs-screenshots`                          | Captures all shots, and writes the images that changed or are missing |
| `bun run docs-screenshots --project profile-viewer` | The same, for the shots that need no manager                          |
| `bun run docs-screenshots -g "top nodes"`           | The same, for the tests whose title matches                           |

| Environment                | The app that the shots load                                                                 |
| -------------------------- | ------------------------------------------------------------------------------------------- |
| None                       | The app that the pipeline manager serves on `localhost:8080`, or at `PLAYWRIGHT_APP_ORIGIN` |
| `DOCS_SCREENSHOTS_BUILD=1` | The app built from this tree and served on port 4173. It calls the API on `localhost:8080`. |

The manager serves the app that it was built with. To capture a UI change that the manager does
not have yet, set `DOCS_SCREENSHOTS_BUILD=1`, or rebuild the manager.

## How a shot stays the same from run to run

| Source of change | Control                                                                                                                                                                                                                                    |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Profile data     | All viewer shots load the same bundle                                                                                                                                                                                                      |
| Shared page      | The app starts once for each worker. The viewer shots share one viewer, and `resetViewer` in `profileViewer.ts` undoes what a test did. The `web-console` shots share one app, and move between its pages with `navigate` in `capture.ts`. |
| Instance state   | A `web-console` shot sees only its own pipelines, and clips the parts of the page that show them                                                                                                                                           |
| Clock and locale | `timezoneId: 'UTC'`, `locale: 'en-US'`                                                                                                                                                                                                     |
| Layout and view  | `profileViewer.ts` waits until the nodes and the view stop moving                                                                                                                                                                          |
| Pointer          | Real pointer events, then the pointer moves off the diagram                                                                                                                                                                                |
| Fonts            | Other fonts can move pixels by more than the threshold. Capture on the same machine each time, or review each changed image.                                                                                                               |

## Files

| File               | Content                                                                                                                                         |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `*.shot.ts`        | One file for each docs page and kind. Images go to `docs.feldera.com/docs/<path>`, from the name that the test gives to `toHaveScreenshot`.     |
| `annotate.ts`      | Frames, arrows, callouts and labels, drawn in an SVG layer from the boxes of live elements. So the marks follow the UI when its layout changes. |
| `capture.ts`       | The name of a docs image, and a clipped compare                                                                                                 |
| `profileViewer.ts` | Opens the bundle, and moves the diagram to a node                                                                                               |

## Not in CI

No CI job captures or checks these shots. Run `bun run docs-screenshots`, look at the images that
changed, and commit the ones that you accept. Revert the others with `git checkout`.
