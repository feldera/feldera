# DBSP documentation

This documentation is built using [Docusaurus 2](https://docusaurus.io/), a
modern static website generator.

### Installation

```bash
yarn
```

You also need the OpenAPI JSON spec from the manager. This file is automatically
generated during the CI build. For local development you need to run once:

```bash
cargo run --bin dbsp_pipeline_manager -- --dump-openapi
jq '.servers= [{url: "http://localhost:8080/v0"}]' openapi.json > openapi_docs.json
```

### Local Development

```bash
yarn dev
```

This command starts a local development server and opens up a browser window.
Most changes are reflected live without having to restart the server.

Format the code & linting:

```bash
yarn format
yarn lint
```

### Build

```bash
yarn build
```

This command generates static content into the `build` directory and can be
served using any static contents hosting service.
