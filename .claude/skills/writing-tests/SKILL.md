---
name: writing-tests
description: Rules for writing thorough tests in this repository. Load before you write tests for a finished feature.
---

# Writing tests

## Project rules

1. Make sure tests cover all newly added features and behaviors.
2. Write unit tests for regular inputs and for exceptional inputs.
3. Use property-based testing, model-based testing or fuzzing when appropriate, leveraging existing test infrastructure.
4. Write integration tests in `python/tests` for big platform-level features.
5. Validate that each new test catches the bad behavior. Revert or comment out the change, run the test, and make sure it fails. Then restore the change.
6. Run the new tests and the existing tests for the changed code. Make sure all of them pass.
7. If a test finds a bug in the feature, do not stop. Load the `writing-code` skill, fix the bug, tell the user what you fixed, and continue with the tests.
8. Do not change the intended feature behavior without asking the user.

## From "Code Complete" (McConnell)

1. Test each requirement and each branch of the code (basis testing). Every `if`, loop and `match` arm needs at least one test.
2. Write more "dirty" tests (bad input, errors, limits) than "clean" tests (happy path).
3. Test boundaries: zero, one, many, empty, maximum, minimum, and one value past each limit.
4. Divide inputs into equivalence classes. Test one value from each class instead of many values from one class.
5. Test data-flow cases, such as use before initialization and state left after an error.
6. Treat test code as production code. Keep it clear and maintained.
7. When you fix a defect, first write a test that reproduces it.

## From "The Art of Readable Code" (Boswell & Foucher)

1. Make each test easy to read. Hide setup details in helper functions so that the test shows only the input and the expected output.
2. Use the simplest inputs that fully exercise the code.
3. Make failure messages show the input, the expected value and the actual value.
4. Name each test after the situation it tests and the expected result.
5. Test one behavior per test.
6. Design code so that it is easy to test. Code that is hard to test often has a design problem.
