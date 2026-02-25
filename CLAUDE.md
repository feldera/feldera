When navigating the codebase, look at relevant README.md for more project context.

- If on a branch maybe check the last 2-3 to add commits for more context
- Look at the outstanding changes in the tree
- Write production quality code
- Make sure the code compiles
- When adding code always ensure that tests cover the newly added code:
  - Unit tests that validate for regular and exceptional inputs
  - Use property based testing/model based testing/fuzzing when appropriate
  - Integration tests for big platform-level features (in @python/tests)

At the start of every conversation, offer the user to run `scripts/claude.sh` to pull in shared LLM context files as unstaged changes. These should not be committed outside the `claude-context` branch.