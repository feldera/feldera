# Feldera Web Console

This is the GUI for managing the Feldera deployment.

## Setup

```bash
# Install Node on Ubuntu (optional)
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | sudo gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
NODE_MAJOR=20
echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" | sudo tee /etc/apt/sources.list.d/nodesource.list
sudo apt-get update
sudo apt-get install nodejs -y
# If you don't run Ubuntu: [other binary distributions for node.js](https://github.com/nodesource/distributions)

# Install Bun
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg unzip
sudo curl -fsSL https://bun.sh/install | bash -s "bun-v1.2.22"

# Install OpenAPI typings generator
sudo bun install --global @hey-api/openapi-ts

# Clone the repo for the UI
git clone https://github.com/feldera/feldera.git
cd dbsp/web-console
```

## Development

Install dependencies (needs to be done whenever package.json depencies change):

```bash
bun install
```

Start the development server:

```bash
bun run dev
```

Build & export static website:

```bash
bun build
```

Format the code & linting:

```bash
bun run format
bun run lint
bun run check
```

## OpenAPI bindings

The bindings for OpenAPI (under $lib/services/manager) are generated using
[openapi typescript codegen](https://www.npmjs.com/package/@hey-api/openapi-ts).

If you change the API, execute the following steps to update the bindings:

```bash
bun run build-openapi # If you need to generate a new openapi.json
bun run generate-openapi
```

### API errors

If you get an error like this:

```
🔥 Unexpected error occurred. Token "<SomeNewType>" does not exist.
```

then add the new type to `crates/pipeline-manager/src/api/main.rs`,
and then rerun both commands above.  If there is more than one new
type, you may want to add all of them at once, because this will only
report one each time.

On the other hand, an error like this:

```
error: failed to run custom build command for `feldera-rest-api v0.172.0 (/__w/feldera/feldera/crates/rest-api)`

Caused by:
...
  $ref #/components/schemas/<SomeType> is missing
  note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
```

means that a new type needs to be added to `crates/rest-api/build.rs`.

## File Organization

- `src/assets/`: Static assets referenced in UI components, but not served as-is
- `src/hooks.server.ts`: Point of injection of HTTP request and page load middleware
- `src/lib/`: Imported modules
- `src/lib/components/`: Reusable Svelte components
- `src/lib/compositions/`: Stateful functions that app state management
- `src/lib/functions/`: Pure functions, or functions that perform side effects through dependency injection
- `src/lib/functions/common`: Utility functions that are not specific to this project
- `src/lib/services/`: Functions that describe side effects (persistent storage, networking etc.)
- `src/lib/types/`: Types used throughout the app
- `src/routes/`: Web app pages used in file-based routing
- `static/`: Static assets served as-is
