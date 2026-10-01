# Agent Instructions

## Before you start

- At the start of every conversation, offer the user to run `scripts/claude.sh`.
  The script pulls in shared LLM context files as unstaged changes.
  Do not commit these files outside the `claude-context` branch.
- To gather more context beyond README.md:
  - Look at the outstanding changes in the tree.
  - If on a branch, check the last 2-3 commits.
  - Look at the relevant README.md in sub-folders.

## Typical user session

A typical session has three steps. Each step has its own rules below.

The step numbers and names are for this file only. Do not use them when you talk to the user. Describe the action instead:
- Not "Let's move to Step 3, cleaning up the comments."
- But "If you are ready, let me go over the comments and clean them up before you commit."

Propose the next step with confidence. Use "let me" or "I will now", not "I can" or "would you like me to".

| Step | What the user does | What the agent does | What the agent does not do | Rules |
|---|---|---|---|---|
| 1. Implement | Designs, implements and iterates on a feature | Writes code, including necessary comments. Makes sure the code compiles after each change. Writes a minimum of high-level, success-path tests to confirm the code works at all. | Write thorough tests (edge cases, error paths, property tests). Exception: an ad-hoc test to check a hypothesis. | [Writing code](#step-1-writing-code) |
| 2. Test | Finishes the feature | Writes thorough tests for the finished feature. Fixes the bugs that the tests find. | Change the intended feature behavior without asking | [Writing tests](#step-2-writing-tests) |
| 3. Comment | Reviews the result | Reviews every comment written or touched in steps 1 and 2. Deletes some, rewrites the others. Adds missing comments and docs. | Add comments that repeat the code | [Writing comments](#step-3-writing-comments-and-docs) |

Moving between steps:

1. The user decides when a feature is finished. Do not guess and move on silently.
2. When you infer that the user is done with a feature (for example, the user asks to commit, open a PR, or "wrap up"), remind the user that the feature does not have thorough tests yet. Ask the user to confirm before you write them. This reminder is very important. Do not skip it.
3. If a test in step 2 finds a bug in the feature, do not stop. Go back to step 1, fix the bug, tell the user what you fixed, and continue with step 2.
4. After tests are done, offer the comment pass (step 3).
5. The user can ask for any step at any time. Follow the user's request over this sequence.

## Step 1: Writing code

Write production quality code. Match the style, naming and idioms of the surrounding code.
Make sure the code compiles. Fix new compiler warnings.
Write comments as necessary. Step 3 applies the comment rules to them.
Write only a minimum of high-level, success-path tests, to confirm that the code is not broken. Also write ad-hoc tests to check a hypothesis. Leave thorough tests for step 2.

### From "The Art of Readable Code" (Boswell & Foucher)

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

### From "Code Complete" (McConnell)

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

## Step 2: Writing tests

Start this step only after the user confirms the feature is finished (see [Typical user session](#typical-user-session)).

### Project rules

1. Make sure tests cover all newly added features and behaviors.
2. Write unit tests for regular inputs and for exceptional inputs.
3. Use property-based testing, model-based testing or fuzzing when appropriate, leveraging existing test infrastructure.
4. Write integration tests in `python/tests` for big platform-level features.
5. Validate that each new test catches the bad behavior. Revert or comment out the change, run the test, and make sure it fails. Then restore the change.

### From "Code Complete" (McConnell)

1. Test each requirement and each branch of the code (basis testing). Every `if`, loop and `match` arm needs at least one test.
2. Write more "dirty" tests (bad input, errors, limits) than "clean" tests (happy path).
3. Test boundaries: zero, one, many, empty, maximum, minimum, and one value past each limit.
4. Divide inputs into equivalence classes. Test one value from each class instead of many values from one class.
5. Test data-flow cases, such as use before initialization and state left after an error.
6. Treat test code as production code. Keep it clear and maintained.
7. When you fix a defect, first write a test that reproduces it.

### From "The Art of Readable Code" (Boswell & Foucher)

1. Make each test easy to read. Hide setup details in helper functions so that the test shows only the input and the expected output.
2. Use the simplest inputs that fully exercise the code.
3. Make failure messages show the input, the expected value and the actual value.
4. Name each test after the situation it tests and the expected result.
5. Test one behavior per test.
6. Design code so that it is easy to test. Code that is hard to test often has a design problem.

## Step 3: Writing comments and docs

This step is a review pass over every comment and doc added or changed in steps 1 and 2. Use `git diff` to find them. For each comment, do one of these:

1. Delete it if the rules below do not require it.
2. Rewrite it if it breaks a rule below.
3. Keep it unchanged only if it complies with all rules below.

Then add the comments that the rules require and that are missing, adhering to the rules below.

### When to write a comment

1. Add a comment only where a reader would need to spend time to infer the meaning of the logic from the code.
2. Add a comment for complex or implicit relationships, for example:
   - an invariant that other code depends on,
   - an ordering or timing requirement,
   - a coupling between distant parts of the code,
   - a non-obvious reason for a choice ("why", not "what"),
   - a workaround for a bug in a dependency,
   - a known flaw (`TODO`, `FIXME`, `HACK`).
3. Do not comment what the code already says. Do not use a comment to explain a bad name. Fix the name.
4. Keep comments short. A one-line comment is often enough.

### What not to write

These are known bad habits of LLMs. Check every comment and doc for them.

1. Do not use jargon that the reader does not need. Use a technical term only if it is the correct name for the concept in this domain or in this codebase.
2. Do not invent jargon or coin new terms. Do not write "the X" for a compound word that you made up and never defined. Do not use words such as "load-bearing", "seam", "gate", "unlock", "smoking gun", "pressure test", "footgun", "hand-waving" or "honest framing" as stand-ins for plain statements.
3. Do not force a common verb onto a niche or exact concept: "which the page asks for", "pipeline remembers the step". "Make the pipeline remember the step" is wrong if the concept is "checkpoint the step". Use the correct term. If no exact term exists, use a widely known technical term. This rule has priority over the STE rule to prefer plain words.
4. Do not write indirectly:
   - No circumlocution. Write "fails if the buffer is full", not "is not in a position to succeed in the case where the buffer has reached capacity".
   - No "It is not X, it is Y" construction. State Y.
   - No sentences that circle a point and then reveal it as an insight. State the point first.
   - No rhetorical questions, no dramatic setups ("Here is the catch:", "The trap:", "The twist:").
5. Do not use metaphors, wordplay or punchy slogans ("keep the signal, govern the response"). Technical text describes, it does not perform.
6. Do not compress text until it is cryptic. Keep the articles, verbs and conjunctions ("and", "but", "because") that make the sentence easy to parse. Do not use a colon or a dash as a substitute for a verb or conjunction.
7. Do not use semicolons, em dashes or emphasis stars.
8. Do not use filler and meta-commentary: "Note that", "It is worth noting", "Importantly", "Basically", "In other words", "As mentioned above".
9. Do not use comparative or retrospective framing when you change behavior or refactor. A comment states what the current code does, as a matter of fact, as if it were always this way. Do not compare it to an old version, and do not narrate the change:
   - No "Now uses a hash map instead of a linear scan", but "Looks up the key in a hash map".
   - No "No longer blocks on flush", "Previously this used X", "Fixed the bug where", "Changed to", "The new approach", "Simplified to".
   - No "unlike before", "still", "now", "anymore", "instead" when they refer to an old version of the code.
   History belongs in the commit message.
10. Do not refer to the conversation, the user or the agent ("as requested", "per the discussion").
11. Do not use words that sound sophisticated where a plain word is exact: "wholesale", "inert", "verbatim", "grain" (for granularity), "surface" (as a verb), "leverage", "utilize".
12. Do not repeat a caveat that is obvious, and do not expand a caveat into a paragraph.
13. Do not copy the style of nearby verbose comments. Match the codebase conventions, but apply these rules to new and touched comments.

### From "The Art of Readable Code" (Boswell & Foucher)

1. Record the thinking behind the code: why this approach, why not the obvious alternative, what surprised you.
2. Explain the value of constants when it is not obvious (for example, why a limit is 64).
3. Anticipate the questions a reader will ask, and the traps a caller can fall into.
4. Give a short big-picture comment for a file, module or complex function when the structure is not obvious.
5. Describe the intent of a block, not its mechanics.
6. Be precise and compact. Avoid ambiguous pronouns such as "it" and "this" when more than one thing can match.
7. Describe corner cases with a concrete input/output example.

### From "Code Complete" (McConnell)

1. Comment at the level of intent. Make comments explain the purpose of the code, not repeat it.
2. Put comments next to the code they describe. Update comments in the same change as the code.
3. Document units, valid ranges and meaning of special values for variables and fields.
4. Document assumptions and preconditions of a routine, and any global effects.
5. Avoid long end-of-line comments. They are hard to maintain.

### From ASD-STE100 Simplified Technical English

Apply these rules to comments and docs. Code identifiers, type names and established domain terms are exempt from word rules.

1. Use one word for one meaning. Pick one term for one concept and use it every time. Do not rotate synonyms such as "input"/"source"/"connector" for the same thing.
2. Use the active voice. Write "The circuit sends the batch", not "The batch is sent". Use the passive voice only if the actor is unknown or not relevant.
3. Keep the modality of every claim. Do not change "may fail" to "fails". Do not add facts, causes or guarantees that are not true.
4. Write one instruction or one idea per sentence. Keep instructions to 20 words or fewer and descriptions to 25 words or fewer.
5. Use simple tenses: simple present, simple past, simple future, and the imperative. Use the present perfect only when it carries current relevance.
6. Use verbs, not nouns made from verbs. Write "analyze the log", not "perform an analysis of the log".
7. Do not use phrasal verbs. Write "start", "remove", "check", not "spin up", "take off", "look into".
8. Do not use semicolons. Split the sentence.
9. Do not stack more than three nouns. Rewrite "output buffer flush timeout value" as "the timeout to flush the output buffer".
10. Do not drop articles, subjects or verbs to save space if the result is ambiguous.
11. Do not use hedge stacks ("it may potentially help to") or marketing adjectives ("seamless", "robust", "powerful", "blazing-fast"). State the claim or give the measurement.
12. Use a list for three or more steps or conditions.
13. Define a domain term the first time you use it if it is not common English.
14. Do not shorten past the point of clarity. The goal is no ambiguity, not the fewest words.

### From "The Elements of Style" (Strunk & White)

1. Omit needless words. Remove "in order to", "the fact that", "it should be noted that", "basically", "simply".
2. Put statements in positive form. Write "is slow", not "is not fast".
3. Use definite, specific, concrete language. Name the actual component, value or condition.
4. Keep related words together. Put a modifier next to the word it modifies.
5. Express parallel ideas in parallel form, especially in lists.
6. Make one paragraph cover one topic, and start it with a topic sentence.
7. Do not overstate. Avoid "very", "rather", "pretty", "quite" and similar qualifiers.
8. Use plain words over fancy ones ("use", not "utilize").
9. Keep one tense in a summary or description.
10. Revise and rewrite. The first draft is rarely the clearest.

### From "Bugs in Writing" (Dupré)

1. Be consistent in terms, capitalization, spelling, number format and notation across a document.
2. Give every pronoun ("it", "this", "they") a clear antecedent. Prefer "this buffer" over a bare "this".
3. Use "that" for restrictive clauses and ", which" for non-restrictive clauses.
4. Use "because" for cause. Use "since" only for time and "while" only for simultaneous events.
5. Use "e.g." for examples and "i.e." for restatements. Do not end an "e.g." list with "etc.".
6. Hyphenate compound adjectives before a noun ("a zero-copy path", "a well-known issue").
7. Avoid "respectively", "the former" and "the latter". They make the reader search back.
8. Do not use jargon or abbreviations that the reader does not know. Define them first.
9. Do not give software human traits it does not have. Write "the scheduler selects", not "the scheduler wants".
10. Use gender-neutral language. Use "they" for a person of unknown gender.
11. Write for the reader. Put the most important information first.
