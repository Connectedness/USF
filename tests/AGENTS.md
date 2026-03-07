# AGENTS.md for Tests

## General Rules

- Unit tests always stay in memory
- Please do not use mocking frameworks like Moq or NSubstitute for test doubles, use hand-crafted test doubles instead
- Do not write nested test classes. All tests should reside in a class which is directly placed in a namespace.
- Use FluentAssertions for assertions
