---
name: unsupported-functions
description: Defines the behavior when encountering Spark constructs that Feldera cannot support. Consult function-reference skill for the full supported/unsupported mapping.
---

# Unsupported Functions

## Purpose

Defines how to handle constructs that cannot be translated to Feldera SQL.

## Core Rule

If a construct is listed as unsupported in the **function-reference** skill, terminate translation for that construct immediately.

- Do NOT approximate it with a different function.
- Do NOT keep retrying repair after a compiler "No match found" error for a known-unsupported function.
- Do NOT silently change semantics to make the SQL compile.
- Do NOT hallucinate error messages or restrictions that don't exist.

## Behavior

When an unsupported construct is found:

1. Translate the rest of the query if possible (schema, supported parts).
2. List each unsupported construct in the `unsupported` array with a brief explanation.
3. If the entire query depends on the unsupported construct, set `feldera_query` to an empty string.
4. Do NOT enter the repair loop for known-unsupported functions.

## Partial translation

If a query has both supported and unsupported parts, translate what you can:

- Schema (CREATE TABLE) can almost always be translated.
- If only one expression in a SELECT is unsupported, note it but translate the rest.
- If the unsupported construct is central (e.g., the main aggregation), return the schema only.

## Examples

Query with unsupported function:
```sql
SELECT id, REGEXP_EXTRACT(url, '(https?://[^/]+)', 1) AS domain FROM logs
```

Response: translate schema, mark `REGEXP_EXTRACT` as unsupported, leave query empty or partial.

Query mixing supported and unsupported:
```sql
SELECT id, UPPER(name) AS name, SOUNDEX(name) AS sound FROM users
```

Response: translate `UPPER(name)`, mark `SOUNDEX` as unsupported.
