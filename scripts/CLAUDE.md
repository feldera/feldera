## Overview

The scripts directory contains scripts commonly used throughout the repository.

## `claude.js` — Merge and Split CLAUDE.md Files

This script manages all `CLAUDE.md` files in the repository by **merging** them into a single root-level `CLAUDE.md` for centralized AI context, and **splitting** them back into their original locations when needed.

### **Commands**
- **Merge**
  - Combines all scattered `CLAUDE.md` files into `<root>/CLAUDE.md`.
  - Each section is wrapped in `<!-- SECTION:<path> START -->` / `<!-- SECTION:<path> END -->` markers, preserving the file’s original relative path (e.g., `benchmark/CLAUDE.md`).
  - If a root `CLAUDE.md` exists before merging, its content is saved as a special “root section” so it can be restored later.
  - Deletes the original scattered files after merging, leaving only the merged root file.

- **Split**
  - Recreates all scattered `CLAUDE.md` files from the merged root file’s sections.
  - Restores the root `CLAUDE.md` content from its saved “root section” if present, or empties it if not.
  - Preserves directory structure when restoring files.

### **Usage**

Run from the repository root through package.json scripts:

```bash
bun run split-claude
bun run merge-claude
```

Run from any subdirectory by passing the root CLAUDE.md path:

```bash
bun ../scripts/claude.js merge --root ..
bun ../scripts/claude.js split --root ..
```

- If `--root` is omitted, the script assumes the repo root is the parent of `/scripts/`.
- Dot-directories like `.github/` are ignored unless explicitly whitelisted in the script.

### **Why Markers?**
The comment markers ensure that:
- Claude can see and reason about each section’s origin path in the raw file.
- The split operation can exactly reconstruct the original files and their locations.