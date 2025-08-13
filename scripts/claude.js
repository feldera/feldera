// /scripts/claude.js
// Merge/split all CLAUDE.md files relative to the project root.
// Root section behavior: On merge, if a pre-merge <root>/CLAUDE.md existed, its content is embedded
// as a special section (relPath === "CLAUDE.md"). On split, that section (if present) becomes the
// new content of <root>/CLAUDE.md; otherwise the root file is cleared.
//
// Usage examples from /web-console:
//   bun ../scripts/claude.js merge
//   bun ../scripts/claude.js split
//   bun ../scripts/claude.js merge --root ..
//   bun ../scripts/claude.js split --root ..

// @ts-check

const fs = require("fs").promises;
const path = require("path");

function parseArgs() {
  const args = process.argv.slice(2);
  const mode = (args[0] || "").toLowerCase();
  let root = null;
  for (let i = 1; i < args.length; i++) {
    const a = args[i];
    if (a === "--root" && args[i + 1]) {
      root = args[i + 1];
      i++;
    } else if (a.startsWith("--root=")) {
      root = a.slice("--root=".length);
    } else if (!a.startsWith("-") && !root) {
      // allow optional positional root after command: e.g. "merge .."
      root = a;
    }
  }
  return { mode, root };
}

const { mode, root } = parseArgs();
const DEFAULT_ROOT = path.resolve(__dirname, "..");
const REPO_ROOT = root ? path.resolve(process.cwd(), root) : DEFAULT_ROOT;
const TARGET_COMBINED = path.join(REPO_ROOT, "CLAUDE.md");

/** @type {Set<string>} */
const IGNORE_DIRS = new Set([
  "node_modules",
  ".git",
  "dist",
  "build",
  "out",
  ".next",
  ".vercel",
  ".svelte-kit",
  ".docusaurus",
  ".venv",
  "coverage",
  ".turbo",
  ".cache",
  ".cargo",
  ".debug",
  "tmp",
]);

const sectionStart = (relPath) => `<!-- SECTION:${relPath} START -->`;
const sectionEnd = (relPath) => `<!-- SECTION:${relPath} END -->`;
const formatSection = (relPath, content) =>
`## Context: ${relPath}
${sectionStart(relPath)}
${content.trimEnd()}
${sectionEnd(relPath)}

---
`

/**
 * @param {string} dir
 * @returns {Promise<string[]>}
 */
async function listClaudeFiles(dir) {
  const out = [];
  const entries = await fs.readdir(dir, { withFileTypes: true });
  for (const entry of entries) {
    if (entry.isDirectory()) {
      if (IGNORE_DIRS.has(entry.name)) continue;
      // if (entry.name.startsWith(".") && entry.name !== ".") continue;
      out.push(...(await listClaudeFiles(path.join(dir, entry.name))));
      continue;
    }
    if (entry.isFile() && entry.name === "CLAUDE.md") {
      const abs = path.join(dir, entry.name);
      // Exclude the root combined target
      if (path.resolve(abs) === path.resolve(TARGET_COMBINED)) continue;
      out.push(abs);
    }
  }
  return out;
}

/** @param {string} p */
function toRel(p) {
  return path.posix.normalize(
    path.relative(REPO_ROOT, p).split(path.sep).join("/")
  );
}

/** @param {string} absTarget */
function ensureInsideRepo(absTarget) {
  const rel = path.relative(REPO_ROOT, absTarget);
  if (rel.startsWith("..")) {
    throw new Error(`Refusing to write outside repo: ${absTarget}`);
  }
}

async function mergeAll() {
  // Capture pre-merge content of root CLAUDE.md (if it exists)
  /** @type {string|null} */
  let preMergeRoot = null;
  try {
    preMergeRoot = await fs.readFile(TARGET_COMBINED, "utf8");
  } catch (e) {
    preMergeRoot = null; // no root file before merge
  }

  const files = await listClaudeFiles(REPO_ROOT);
  files.sort((a, b) => toRel(a).localeCompare(toRel(b)));

  const sections = [];

  // If a pre-merge root existed, include it as an (optional) root section
  if (preMergeRoot !== null) {
    const rel = "CLAUDE.md"; // special: points to the root file itself
    const block = formatSection(rel, preMergeRoot);
    sections.push(block);
  }

  // Include all scattered CLAUDE.md files
  for (const abs of files) {
    const rel = toRel(abs);
    const content = await fs.readFile(abs, "utf8");
    const block = formatSection(rel, content);
    sections.push(block);
  }

  await fs.writeFile(TARGET_COMBINED, sections.join("\n"), "utf8");
  console.log(
    `Merged ${files.length} CLAUDE.md file(s) into ${toRel(TARGET_COMBINED)} using root ${REPO_ROOT}.`
  );

  // Delete merged files (keep only the root combined file)
  for (const abs of files) {
    try {
      await fs.unlink(abs);
      console.log(`Deleted ${toRel(abs)}`);
    } catch (err) {
      console.warn(`Failed to delete ${toRel(abs)}:`, err);
    }
  }
}

async function splitAll() {
  // Read combined
  let combined;
  try {
    combined = await fs.readFile(TARGET_COMBINED, "utf8");
  } catch (err) {
    if (/** @type {any} */ (err).code === "ENOENT") {
      console.error(`No ${toRel(TARGET_COMBINED)} found. Run "merge" first or set --root correctly.`);
      process.exit(1);
    }
    throw err;
  }

  // Extract all sections (including optional root section with relPath === "CLAUDE.md")
  const re = /<!-- SECTION:(.*?) START -->\s*([\s\S]*?)\s*<!-- SECTION:\1 END -->/g;

  /** @type {RegExpExecArray | null} */
  let match;
  let count = 0;
  let rootAssigned = false;

  while ((match = re.exec(combined)) !== null) {
    const relPath = match[1].trim();
    const body = match[2];
    const absPath = path.join(REPO_ROOT, relPath);
    ensureInsideRepo(absPath);

    if (path.posix.basename(relPath) !== "CLAUDE.md") {
      console.warn(`Skipping section with non-CLAUDE.md filename: ${relPath}`);
      continue;
    }

    await fs.mkdir(path.dirname(absPath), { recursive: true });
    await fs.writeFile(absPath, body.trimStart(), "utf8");
    console.log(`Wrote ${relPath}`);
    count++;

    if (relPath === "CLAUDE.md") {
      rootAssigned = true; // we restored the root content from the optional root section
    }
  }

  // If no explicit root section was found, clear the root file
  if (!rootAssigned) {
    await fs.writeFile(TARGET_COMBINED, "", "utf8");
    console.log(`No root section found; cleared ${toRel(TARGET_COMBINED)}.`);
  }

  if (count === 0) {
    console.warn("No sections found to split. Are the markers intact and --root correct?");
  } else {
    console.log(
      `Restored ${count} CLAUDE.md file(s) from ${toRel(TARGET_COMBINED)} using root ${REPO_ROOT}.`
    );
  }
}

(async function main() {
  if (mode === "merge") {
    await mergeAll();
  } else if (mode === "split") {
    await splitAll();
  } else {
    console.log(`Usage:
  bun claude.js merge [--root <path>]   # Combine all CLAUDE.md into <root>/CLAUDE.md (optional root section) and delete originals
  bun claude.js split [--root <path>]   # Recreate individual CLAUDE.md files; root set from root section or cleared
`);
    process.exit(1);
  }
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
