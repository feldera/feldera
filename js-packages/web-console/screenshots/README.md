# Docs screenshots

These Playwright tests capture the web console screenshots into `docs.feldera.com/docs`. A test sets
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
| `bun run check:docs-screenshots`                    | Captures all shots, and fails if an image changed. Writes no image.   |

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
| Fonts            | CI uses the pinned `mcr.microsoft.com/playwright` image. Other fonts can move pixels by more than the threshold.                                                                                                                           |

## Files

| File               | Content                                                                                                                                         |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `*.shot.ts`        | One file for each docs page and kind. Images go to `docs.feldera.com/docs/<path>`, from the name that the test gives to `toHaveScreenshot`.     |
| `annotate.ts`      | Frames, arrows, callouts and labels, drawn in an SVG layer from the boxes of live elements. So the marks follow the UI when its layout changes. |
| `capture.ts`       | The name of a docs image, and a clipped compare                                                                                                 |
| `profileViewer.ts` | Opens the bundle, and moves the diagram to a node                                                                                               |

## CI

| When                                  | Workflow                                     | What it does                                                                                                                                                                                                   |
| ------------------------------------- | -------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| A pull request changes `js-packages/` | `docs-screenshots.yml`, job `profile-viewer` | Captures the `profile-viewer` shots. If an image changed, commits it as `[docs] Update web-console screenshots` on the PR branch. It does not run on pull requests from forks, because it cannot push to them. |
| You start it from the Actions tab     | `docs-screenshots.yml`, job `all`            | Captures all shots against a manager service from the image tag that you give (default `latest`), and commits the changes                                                                                      |
| The merge queue                       | `test-web-console-e2e.yml`                   | Checks all shots against the manager of the commit. If a shot is stale, the merge fails.                                                                                                                       |

When the merge queue fails on a shot, start the workflow on the branch, or run
`bun run docs-screenshots` against your local manager and commit the images. If the branch changes
the API, the `latest` image may not work for the `all` job. Then use a local manager that you built
from the branch.
