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

A typical session has three steps. Before you start a step, load its skill. The skill has the rules for that step.

| Step | What the user does | What the agent does | Skill |
|---|---|---|---|
| 1. Implement | Designs, implements and iterates on a feature | Writes code and comments. Writes only minimal success-path tests, and a reproducing test for a bug fix. | `writing-code` |
| 2. Test | Finishes the feature | Writes and runs thorough tests. If a test finds a bug, returns to step 1 to fix it. | `writing-tests` |
| 3. Comment | Reviews the result | Reviews every comment and doc written or touched in steps 1 and 2. Deletes some, rewrites the others, adds missing ones. | `writing-comments` |

The skills are in `.claude/skills/<name>/SKILL.md`.

Moving between steps:

1. The user decides when a feature is finished. Do not guess and move on silently.
2. When you infer that the user is done with a feature (for example, the user asks to commit, open a PR, or "wrap up"), remind the user that the feature does not have thorough tests yet. Ask the user to confirm before you write them. This reminder is very important. Do not skip it.
3. If a test in step 2 finds a bug in the feature, do not stop. Go back to step 1, fix the bug, and continue with step 2.
4. After tests are done, offer the comment review (step 3).
5. The user can ask for any step at any time. Follow the user's request over this sequence.
6. If no user can answer (CI jobs, review bots, one-shot tasks such as "implement X and open a PR"), do all three steps without asking.

Talking to the user about steps:

- The step numbers and names are for this file only. Do not use them when you talk to the user. Describe the action instead:
  - Not "Let's move to Step 3, cleaning up the comments."
  - But "If you are ready, let me go over the comments and clean them up before you commit."
- Propose the next step with confidence. Use "let me" or "I will now", not "I can" or "would you like me to".
