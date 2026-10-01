---
name: writing-code
description: Rules for writing production code in this repository. Load before you write or change code.
---

# Writing code

## Project rules

1. Write production quality code. Match the style, naming and idioms of the surrounding code.
2. Make sure the code compiles after each change. Fix new compiler warnings.
3. Write comments as necessary. A later review pass applies the comment rules to them.
4. Write only a minimum of high-level, success-path tests, to confirm that the code is not broken. Also write ad-hoc tests to check a hypothesis. Leave thorough tests until the user finishes the feature.
5. When you fix a bug, first write a test that reproduces the bug. Make sure it fails before the fix and passes after it. This is an exception to rule 4.

## From "The Art of Readable Code" (Boswell & Foucher)

1. Write code that minimizes the time another person needs to understand it. This goal beats "short" and "clever".
2. Put information into names:
   - Pick specific words (`fetch`, `download`, `parse`) over generic ones (`get`, `do`, `handle`).
   - Avoid empty names such as `tmp`, `retval`, `data`, `foo`, except in the smallest scopes.
   - Attach units and important attributes: `timeout_ms`, `size_bytes`, `unsafe_html`, `plaintext_password`.
   - Make name length match scope size. Short names for small scopes, long names for wide scopes.
3. Choose names that cannot be misread:
   - Use `min`/`max` for inclusive limits, `first`/`last` for inclusive ranges, `begin`/`end` for half-open ranges.
   - Name booleans `is_`, `has_`, `can_`, `should_`. Avoid negated names such as `disable_x` or `not_found`.
   - Match user expectations. For example, `get_x()` must be cheap. Use `compute_x()` for expensive work.
4. Simplify control flow:
   - Return early. Handle error and edge cases first with guard clauses.
   - Keep nesting shallow.
   - In comparisons, put the changing value on the left and the stable value on the right.
   - Handle the positive, simple or interesting case first in `if`/`else`.
   - Use the ternary operator only for simple cases.
5. Break down big expressions:
   - Add explaining variables for sub-expressions with a meaning.
   - Add summary variables for repeated or long conditions.
   - Use De Morgan's laws to simplify boolean logic.
   - Do not use clever short-circuit tricks.
6. Reduce variables and their scope:
   - Remove intermediate variables that add no meaning.
   - Make scope as small as possible. Avoid global and wide-scope mutable state.
   - Prefer variables that are written once.
7. Extract unrelated subproblems into helper functions or generic utilities. Keep the high-level goal of a function visible.
8. Do one task at a time. Split code that does several tasks into separate blocks or functions.
9. Before you write complex logic, describe it in plain English. Then write the code that follows the description.
10. Write less code:
    - Do not build features that nobody asked for.
    - Reuse the standard library and existing project code.
    - Remove dead code and unused parameters.
11. Keep layout consistent. Make similar code look similar. Group related lines into paragraphs.

## From "Code Complete" (McConnell)

1. Managing complexity is the primary technical goal. Every design choice must reduce the amount of the system a reader needs to keep in mind at once.
2. Write code for the reader first and the compiler second. Code is read much more often than it is written.
3. Hide information. Put design decisions that can change behind an interface. Keep each module, class and routine at one consistent level of abstraction.
4. Give each routine one clear purpose (strong cohesion). Keep dependencies between routines small and explicit (loose coupling). Keep parameter lists short, about seven or fewer.
5. Handle errors on purpose:
   - Validate data at the system boundary ("barricade"). Code inside the barricade can trust its inputs.
   - Use assertions for conditions that must never happen. Use error handling for conditions that can happen.
   - Pick one error-handling strategy per area and use it consistently.
   - Do not swallow errors silently.
6. Use each variable for one purpose only. Declare and initialize variables close to their first use. Replace magic numbers and strings with named constants.
7. Make data structures carry the logic where possible. Use table-driven code instead of long `if`/`match` chains when the table is clearer.
8. Design before you code. For non-trivial routines, sketch the steps first, then write code under each step, then remove steps that the code now makes clear.
9. Refactor in small, safe steps. Do not mix a refactor with a behavior change in the same step.
10. Debug with the scientific method:
    - Reproduce the defect.
    - Form a hypothesis and test it.
    - Fix the root cause, not the symptom.
    - Understand the defect before you change code.
    - Look for the same defect elsewhere.
11. Get the code correct and clear first. Measure before you optimize. Optimize only the measured hot spots.
12. Build and integrate in small increments. Keep the code compiling and working after each increment.
13. Use layout to show the logical structure of the code. Follow the existing formatter and conventions of the project.
