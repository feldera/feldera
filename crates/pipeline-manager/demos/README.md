# Packaged SQL-only demos

The SQL of packaged demo starts with a preamble to convey explicitly the
title, name and description. This information is for example extracted by the
Web Console for display on the demos overview.

All the SQL files in `sql/` are packaged demos with every Feldera release.

- **Preamble validation:**
  ```bash
  uv run validate-preamble.py sql/*.sql
  ```

- **Specify specific demos directory:**
  ```bash
  cargo run --bin pipeline-manager \
            -- --demos-dir crates/pipeline-manager/demos/sql
  ```

- **Run all demos packaged on the Feldera instance:**
  ```bash
  uv run run.py --api-url http://localhost:8080
  ```

## Specification

The format is as follows:

```
-- [title] ([pipeline-name])
--
-- [description-line1]
-- [description-line2]
-- ...
-- [description-lineN]
--
[other-sql]
```

... with the following constraints:

- `[title]` is non-empty and at most 100 characters.
- `[pipeline-name]` adheres to the name constraints: at most 100 characters and follow pattern `[a-zA-Z0-9_-]+`.
- There must be at least one `[description-line]`.
- All `[description-line]` joined together as description is non-empty and at most 1000 characters.
- The whitespace at the end of each preamble line is ignored.
- The empty comment line in-between title/pipeline-name and description,
  and after the description, is mandatory.

**How to validate: regular expression**
```regexp
^-- (.+) \(([a-zA-Z0-9_-]+)\)[ \t]*\r?\n--[ \t]*\r?\n((-- .+\r?\n)+)--[ \t]*\r?\n
```
... with afterward:
- First group is title, second group is name, and third group is description lines.
- The description lines can then be parsed by finding in it all regex matches of `-- .+\r?\n`,
  of each removing the `-- ` prefix and trimming whitespace, finally join with
  a space character (` `), and trim whitespace.
- Trim whitespace from the title, and check it is non-empty and at most 100 characters
- Check name is at most 100 characters
- Check description is at most 1000 characters

## Example

```sql
-- Example (example-1)
--
-- This is a description of the example demo
-- which must span at least one line.
--
-- More comments not part of the description
-- can be written.

-- Explanation about the table
CREATE TABLE example ( col1 INT );
```
