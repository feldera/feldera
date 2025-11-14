# Feldera Profiler

Interactive visualization tool for analyzing DBSP circuit performance in Feldera pipelines.

## Quick Start

You need `npm` installed to run the profiler.

```bash
./run.sh
```

Open your browser to the URL shown (typically `http://localhost:5173`).

## What It Does

Visualizes two complementary datasets as an interactive graph:

1. **Profile data**: Runtime performance metrics (execution time, memory, throughput, cache stats)
2. **Dataflow graph**: Circuit structure with SQL source mappings

## Getting Profile Data

### 1. Generate Profile Data

Run a Feldera pipeline with profiling enabled:

```bash
# Via REST API
curl -X POST http://localhost:8080/v0/pipelines/{pipeline_name}/circuit_profile > data/my-profile.json

# Or via Python SDK
from feldera import FelderaClient
client = FelderaClient("http://localhost:8080")
profile = client.get_pipeline("my_pipeline").profile()
```

Save the response to `data/my-profile.json`.

### 2. Generate Dataflow Graph

Compile your SQL program with the `--dataflow` flag using the SQL-to-DBSP compiler:

```bash
cd sql-to-dbsp-compiler
./SQL-compiler/sql-to-dbsp \
  --input /path/to/your-program.sql \
  --dataflow ../profiler/data/dataflow-my-profile.json
```

This generates a JSON file containing the circuit structure with SQL source position mappings.

### 3. Update Entry Point

Edit `src/index.ts` to load your files:

```typescript
Globals.getInstance().loadFiles("data", "my-profile");
```

The profiler will load:
- `data/my-profile.json` (profile)
- `data/dataflow-my-profile.json` (dataflow graph)

## Sample Data

The `data/` directory includes sample files (`rec.json` and `dataflow-rec.json`) for testing.
