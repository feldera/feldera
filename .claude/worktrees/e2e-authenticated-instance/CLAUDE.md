- To gather more context beyond README.md:
  - Look at the outstanding changes in the tree
  - If on a branch check the last 2-3 to commits
  - Look at relevant README.md in sub-folders

- Write production quality code.
  - Adhere to rules in "Code Complete" by Steve McConnell.
  - Adhere to rules in "The Art of Readable Code" by Dustin Boswell & Trevor Foucher.
- Make sure the code compiles.
- When adding code always ensure that tests cover the newly added code:
  - Unit tests that validate for regular and exceptional inputs
  - Use property based testing/model based testing/fuzzing when appropriate
  - Integration tests for big platform-level features (in @python/tests)

- Add/update documenation and comments.
  - Adhere to rules in "Bugs in Writing: A Guide to Debugging Your Prose" by Lyn Dupre.
  - Adhere to rules in "The Elements of Style, Fourth Edition" by William Strunk Jr. & E. B. White

At the start of every conversation, offer the user to run `scripts/claude.sh` to pull in shared LLM context files as unstaged changes.
These should not be committed outside the `claude-context` branch.
