# DBSP Web UI

This is the web UI for the DBSP project.

## Setup

```bash
sudo apt-get update
sudo apt-get install git sudo curl
curl -fsSL https://deb.nodesource.com/setup_19.x | sudo -E bash - &&\
sudo apt-get install -y nodejs
npm install --global yarn
npm install --global openapi-typescript-codegen

git clone https://github.com/vmware/database-stream-processor.git
cd database-stream-processor/web-ui
```

If you don't run ubuntu: [other binary distributions for node.js](https://github.com/nodesource/distributions)

## Development

Install dependencies (needs to be done whenever package.json depencies change):

```bash
yarn install
```

Start the development server:

```bash
yarn dev
```

Build & export static website:

```bash
yarn build
yarn export
```

Format the code & linting:

```bash
yarn format
yarn lint
```

## OpenAPI bindings

The bindings for OpenAPI (under src/types/manager) are generated using
[openapi typescript codegen](https://www.npmjs.com/package/openapi-typescript-codegen).

If you change the API, execute the following steps to update the bindings:

```bash
yarn generate-openapi
```

Note sometimes strange caching errors may warrant deleting `node_modules` after
regenerating the API bindings.

## File Organization

- `@core`: Settings, style and overrides.
- `analytics`: Logic to handle projects (SQL editor, programs).
- `components`: contains reusable React components.
- `configs`: Theme configuration (see also Material UI themes).
- `data`: Logic for handling connectors (Add, Update, Delete).
- `home`: Home page.
- `layouts`: General layout components.
- `navigation`: Side-bar Menu.
- `pages`: accessible website pages (e.g. `pages/index.tsx` is the homepage)
- `streaming`: Logic for building, managing Pipelines.
- `types`: Types used throughout the app, OpenAPI generated types.
- `utils`: contains utility functions.
