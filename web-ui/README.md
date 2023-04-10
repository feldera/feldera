# DBSP Web UI

This is the web UI for the DBSP project.

## Setup

TODO

## Development

Install dependencies (needs to be done whenever package.json depencies change):

```bash
yarn install
```

Recompile the OpenAPI client code (needs to be done whenever the OpenAPI spec
changes):

```bash
yarn openapi
```

Start the development server:

```bash
yarn dev
```

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
