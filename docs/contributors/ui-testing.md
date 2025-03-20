# Web Console testing

Regression UI testing is performed using Playwright framework.
The repository contains end-to-end (e2e) tests that are to be executed against a clean Feldera instance,
and standalone unit (ct) tests that include UI component tests and TypeScript function tests.
Existing Playwright tests are executed during CI and can be run manually within provided devcontainer environment.
Add environment variable `CI=true` when executing tests in CI setting.

### Running e2e tests

Run `yarn test-e2e` to execute all e2e tests on all supported platforms in background, or run `yarn test-e2e-ui` to open a UI to run tests interactively.
Tests should be executed against a running Pipeline Manager instance.
As an artificial limitation of scope, currently no services for Kafka, Debezium, Snowflake and other similar connector types are available for tests in the CI, so only HTTP connectors and API is available along with the UI itself.

### Running ct tests

Run `yarn test-ct` to execute all ct tests on all supported platforms in background, or run `yarn test-ct-ui` to open a UI to run tests interactively.
Unit tests do not need Feldera instance to run because they run against individual ESM modules compiled on-demand for the test.

### Contributing tests

The tests directory is `feldera/web-console/tests`.
Regression testing is performed by comparing snapshots with the test results.
Snapshots are expected in the directory `feldera/web-console/playwright-snapshots` and are generated during test run if they are not found.
Snapshots are not a part of the feldera repo, and are instead stored at https://github.com/feldera/playwright-snapshots.
During CI, snapshots of the correct version are copied into the build directory.
Environment variable `PLAYWRIGHT_SNAPSHOTS_COMMIT` in `feldera/.arg` specifies the commit hash being used in CI to test against.
When committing new tests or updating screenshots for existing tests, `PLAYWRIGHT_SNAPSHOTS_COMMIT` needs to be updated as well.
When testing locally, you need to manually clone `playwright-snapshots` and checkout the correct commit hash, e.g.:

```
cd web-console && yarn test-prepare
```
OR
```
cd web-console
npm i -g degit
degit feldera/playwright-snapshots#{commit_hash}
```
OR
```
cd web-console
git clone https://github.com/feldera/playwright-snapshots && git checkout {commit_hash}
```


### Writing tests

If you want to use `Playwright codegen` to automatically create new tests from UI interactions,
install Playwright on your host system: https://playwright.dev/docs/intro

Execute Playwright Codegen with:
```bash
yarn playwright codegen http://127.0.0.1:8080/
```

Keep in mind that codegen is not designed to produce production-ready code,
so you might need to edit it.

Prefer using `data-testid` prop and `.getByTestId()` to locate the elements.
When that is inconvenient, consider `.getByRole()`.
Resort to visible text-based locators when above methods are inconvenient.
Avoid locating by HTML element names, CSS classes and `id` prop.
When it is impractical to decorate a concrete HTML element with `data-testid` prop -
decorate its wrapping element, and then seek from this wrapper by target role, HTML element name or label.

Before all tests, a cleanup procedure defined in tests/global.setup.ts is executed.
Each test expects a clean Pipeline Manager state, and should contain its own cleanup procedure.

You should use `test.step()` API to semantically group the procedures in your test.

The following naming scheme is used for snapshots:
'A-B-DESCRIPTION.png', where
- A is an index number of a current `test.step()`;
- B is an index number of a snapshot in a current test;
- DESCRIPTION is a short description of observed view.