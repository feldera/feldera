# common-lib

Framework-independent TypeScript utilities shared by Feldera's frontends (the
web console, the profiler app and their libraries). Nothing here depends on
Svelte or on a specific app, so any package can use it, including plain
TypeScript libraries and Node scripts. Svelte components belong in
[`common-ui`](../common-ui/README.md).

## Usage

The package has no root entry point. Import each module by its path, so that an
app bundles only the modules and the third-party libraries that it uses:

```ts
import { groupBy } from 'common-lib/array'
import { formatQty } from 'common-lib/format'
import type { SameNullability } from 'common-lib/types/nullable'
```

## Modules

| Module | Contents | Needs |
|---|---|---|
| `array`, `enum`, `function`, `math`, `object`, `percent`, `stream`, `string`, `tuple`, `union` | Generic helpers | |
| `types/function`, `types/nullable` | Generic helper types | |
| `duration` | The `Microseconds` unit type | |
| `promise` | Promise and timer helpers | `worker-timers` |
| `date` | Date helpers | `dayjs` |
| `format` | Number, duration and date formatting | `d3-format`, `dayjs` |
| `color` | Theme color helpers | `colorizr` |
| `bigNumber` | `BigNumber` helpers | `bignumber.js` |
| `valibot` | `BigNumber` schemas for Valibot | `bignumber.js`, `valibot` |
| `d3-random-bignumber` | Random `BigNumber` generators | `bignumber.js` |
| `felderaRelation` | SQL relation name normalization | |
| `latencyColor` | Color scale for connector latencies | `tiny-invariant` |

The libraries in the "Needs" column are optional peer dependencies. A package
that imports a module must also depend on the libraries that the module needs.

## Developing

```sh
bun run check   # type-check, including the tests
bun run test    # unit tests
bun run build   # compile to dist/
```

Consumers import the built output (`dist/`). `bun install` builds this package
through the `prepare` script. The `dev`, `build`, `check` and test scripts of
every package that uses `common-lib` start with `bun run deps`. This script
rebuilds `common-lib` and the other workspace libraries that the package needs.
A running dev server does not get changes that you make after it starts. To use
them, rebuild this package and reload the page:

```sh
bun --cwd=js-packages/common-lib run build   # from the repo root
```

Relative imports inside `src/` use the `.ts` extension. `tsc` rewrites them to
`.js` in `dist/`, so the output is valid ES modules.
