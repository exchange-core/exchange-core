# Repository Guidelines

## Project Structure & Module Organization

The Java engine lives in `src/main/java/exchange/core2/core`; Java tests and fixtures are under `src/test/java` and `src/test/resources`. The Zig port is rooted at `src/zig`: `core/` contains exchange logic, `apps/` contains TCP and browser entry points, `bench/` contains performance harnesses, `tools/` contains parity checks, and `fixtures/` stores cross-language evidence. Architecture guides are in `docs/java-flows/`. Maven outputs belong in `target/`; Zig outputs and browser bundles belong in `zig-out/`. Do not commit cache directories or generated dump files.

## Build, Test, and Development Commands

- `mvn test` runs the standard JUnit 5 Java suite; the POM emits Java 8-compatible bytecode.
- `mvn -Pit test` selects integration tests named `IT*.java` or `*IntegrationTest.java`.
- `mvn install` tests and installs the Java artifact into the local Maven repository.
- `zig fmt --check build.zig src/zig` checks all Zig formatting.
- `zig build test` runs Debug Zig unit tests; `zig build test-release-safe` repeats them in ReleaseSafe mode.
- `zig build -j1 gate --summary all` runs the complete serial migration, parity, API-audit, and browser gate. Keep `-j1`: persistence tests share Java-compatible artifact names.
- `zig build run-server` starts the TCP demo; in another terminal, try `zig build run-client -- ping`.

Use Zig 0.16.0, as pinned by `build.zig.zon` and CI. A compatible JDK 8 or 11 avoids newer-JDK OpenHFT reflection failures.

## Coding Style & Naming Conventions

Use four-space indentation. Java classes use `UpperCamelCase`, methods and fields use `lowerCamelCase`, and packages mirror their directory paths. Zig files and functions use `snake_case`; public types use `UpperCamelCase`. Run `zig fmt` on changed Zig files. Preserve explicit error sets, exhaustive command/event switches, deterministic ownership, and Java wire enum/integer mappings.

## Testing Guidelines

Java uses JUnit 5, Mockito, Hamcrest, and Cucumber. Name unit tests `*Test.java` and integration tests `IT*.java`. Zig tests stay near their implementation as descriptive `test "..."` blocks. No numeric coverage threshold is configured; every behavioral change should add a focused regression test. Changes affecting Java/Zig parity must also pass the full gate and update the relevant audit or `src/zig/MIGRATION_COVERAGE.md` evidence.

## Commit & Pull Request Guidelines

Recent commits follow concise Conventional Commit subjects such as `fix(zig): preserve persistence callback fanout`; use an imperative `type(scope): summary` when practical. Keep commits narrowly scoped. Pull requests should explain behavior and compatibility impact, link issues, list exact verification commands, and identify generated parity artifacts. Include screenshots for browser UI changes and benchmark evidence only for performance claims.

## cx — Semantic Code Navigation

Prefer cx over reading files. Zoom in until you have what you need, then stop:
`overview` > `symbols` > `definition` or `references`.

Fall back to the Read tool when `cx` can't represent the target (anonymous functions, JSX-inline components, dynamic
dispatch, string-keyed lookups, non-symbol regions).

### Common Recipes & Extra Info

```
cx overview DIR --full                               `--full` also includes kind/range/signature for direct files
cx symbols [--kind K] [--name GLOB] [--file PATH]    search symbols project-wide
cx symbols --kinds [--file PATH]                     list distinct kinds with counts
cx definition --name NAME [--from PATH] [--kind K]   get a definition (optionally filtered to specific kind, e.g. function body)
cx references --name NAME [--file PATH] [--context]  find usages, `--context` will show the line where the use appears
```

- `--file` and `--from` are identical and restrict the symbol to a precise file, but only if there is an exact match for
  the path resolved from cwd.
- Kinds: fn, struct, enum, trait, type, const, class, interface, module, event, field, heading
- `.gitignore` is honored. Untracked-but-not-ignored files are still indexed.

### Key patterns

- Start with `cx overview .`, drill into subdirectories — cheaper than ls + reading files.
- Can't find a symbol that should exist? Make sure the language grammar is installed via `cx lang list`.
- `cx definition --name X` gives exact text for Edit tool's `old_string` without reading the whole file.
- `cx references --name X` groups hits by file; add `--context` only when exact source lines are needed.
- When re-entering an unfamiliar area or picking up a topic after a gap, use `cx overview` / `cx definition` to
  re-orient — don't re-read full files
- Check signatures for `pub`/`export` to identify public API without reading the file.
- If your harness restricts writes outside the project root, the default cache location won't be writable.
  Set `$CX_CACHE_DIR` to a project-local path, or grant write permission to the default cache dir.

### Pagination

Default limits: overview: unlimited, definition 3, symbols 100, references 50.

When truncated, stderr shows: `cx: 3/32 definitions for "X"`. Use `--file` (or `--from`) and `--kind` to narrow, or use
`--offset` to get further pages, or `--all` to get all results.

When not limited, `--json` returns an array of results. When limited, it returns an object with
shape `{total, offset, limit, results: <array containing $limit results>}`.
