---
name: writing-comments
description: Rules for the review pass over comments and docs in this repository. Load before you review or write comments and docs.
---

# Writing comments and docs

This is a review pass over every comment and doc added or changed while the feature was written and tested. Use `git diff` to find them. Check each comment against all rules in this file. For each comment, do one of these:

1. Delete it if the rules do not require it.
2. Rewrite it if it breaks a rule.
3. Keep it unchanged only if it complies with all rules.

Then add the comments that the rules require and that are missing.

## When to write a comment

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

## What not to write

These are known bad habits of LLMs. Check every comment and doc for them.

1. Do not use jargon that the reader does not need. Use a technical term only if it is the correct name for the concept in this domain or in this codebase.
2. Do not invent jargon or coin new terms. Do not write "the X" for a compound word that you made up and never defined. Do not use words such as "load-bearing", "seam", "gate", "unlock", "smoking gun", "pressure test", "footgun", "hand-waving" or "honest framing" as stand-ins for plain statements.
3. Do not force a common verb onto a niche or exact concept: "which the page asks for", "pipeline remembers the step". "Make the pipeline remember the step" is wrong if the concept is "checkpoint the step". Use the correct term. If no exact term exists, use a widely known technical term. This rule has priority over the STE rule below to prefer plain words.
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

## From "The Art of Readable Code" (Boswell & Foucher)

1. Record the thinking behind the code: why this approach, why not the obvious alternative, what surprised you.
2. Explain the value of constants when it is not obvious (for example, why a limit is 64).
3. Anticipate the questions a reader will ask, and the traps a caller can fall into.
4. Give a short big-picture comment for a file, module or complex function when the structure is not obvious.
5. Describe the intent of a block, not its mechanics.
6. Be precise and compact. Avoid ambiguous pronouns such as "it" and "this" when more than one thing can match.
7. Describe corner cases with a concrete input/output example.

## From "Code Complete" (McConnell)

1. Comment at the level of intent. Make comments explain the purpose of the code, not repeat it.
2. Put comments next to the code they describe. Update comments in the same change as the code.
3. Document units, valid ranges and meaning of special values for variables and fields.
4. Document assumptions and preconditions of a routine, and any global effects.
5. Avoid long end-of-line comments. They are hard to maintain.

## From ASD-STE100 Simplified Technical English

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

## From "The Elements of Style" (Strunk & White)

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

## From "Bugs in Writing" (Dupré)

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
