# Root Agents.md

USF is a lightweight framework for modern cloud-native applications. It consists of an asynchronous messaging system
and a saga framework.

## Implementation rules

Plans typically have acceptance criteria with check boxes. Check each box when you are finished with the corresponding
criterion.

## General Rules for the Code Base

- Implicit usings or global usings are not allowed - use explicit using statements for clarity.
- The library is not published in a stable version yet, you can make breaking changes.
- `<TreatWarningsAsErrors>` is enabled in Release builds, so your code changes must not generate warnings.
- If it is properly encapsulated, make it public. We don't know how callers would like to use this library. When some
  types are internal, this might make it hard for callers to access these in tests or when making configuration changes.
  Prefer public APIs over internal ones.

## Plan Rules

Read ./ai-plans/AGENTS.md for details on how to write plans.

## Test Rules

Read ./tests/AGENTS.md for details on how to write tests.

## Here is Your Space

If you encounter something worth noting while you are working on this code base, write it down here in this section.
Once you are finished, I will discuss it with you and we can decide where to put your notes.
