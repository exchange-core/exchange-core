# Verification Guide

Before marking any task complete, you MUST verify your work. Verification separates "I think it's done" from "it's actually done."

## The Verification Process

1. **Re-read the task description**: What did you originally commit to do?
2. **Check acceptance criteria**: Does your implementation satisfy the "Done when" conditions?
3. **Run relevant tests**: Execute the test suite and document results
4. **Test manually**: Actually try the feature/change yourself
5. **Compare with requirements**: Does what you built match what was asked?

## Strong vs Weak Verification

### Strong Verification Examples

- ✅ "All 60 tests passing, build successful"
- ✅ "All 69 tests passing (4 new tests for middleware edge cases)"
- ✅ "Manually tested with valid/invalid/expired tokens - all cases work"

### Weak Verification (Avoid)

- ❌ "Should work now" — "should" means not verified
- ❌ "Made the changes" — no evidence it works
- ❌ "Added tests" — did the tests pass? What's the count?
- ❌ "Fixed the bug" — what bug? Did you verify the fix?

## Verification by Task Type

| Task Type     | How to Verify                                           |
| ------------- | ------------------------------------------------------- |
| Code changes  | Run full test suite, document passing count             |
| New features  | Run tests + manual testing of functionality             |
| Configuration | Test the config works (run commands, check workflows)   |
| Documentation | Verify examples work, links resolve, formatting renders |
| Refactoring   | Confirm tests still pass, no behavior changes           |

## Cross-Reference Checklist

Before marking complete, verify all applicable items:

- [ ] Task name requirements met
- [ ] Description "Done when" criteria satisfied
- [ ] Tests passing (document count: "All X tests passing")
- [ ] Build succeeds (if applicable)
- [ ] Manual testing done (describe what you tested)
- [ ] No regressions introduced
- [ ] Edge cases considered (error handling, invalid input)
- [ ] Follow-up work identified (created new tasks if needed)

**If you can't check all applicable boxes, the task isn't done yet.**

## Result Examples with Verification

### Code Implementation

```bash
dex complete xyz789 --commit a1b2c3d --result "Implemented JWT middleware:

Implementation:
- Created src/middleware/verify-token.ts
- Separated 'expired' vs 'invalid' error codes

Verification:
- All 69 tests passing (4 new tests for edge cases)
- Manually tested with valid token: ✅ Access granted
- Manually tested with expired token: ✅ 401 with 'token_expired'
- Manually tested with invalid signature: ✅ 401 with 'invalid_token'"
```

### Configuration/Infrastructure

```bash
dex complete abc456 --result "Added GitHub Actions workflow for CI:

Implementation:
- Created .github/workflows/ci.yml
- Jobs: lint, test, build with pnpm cache

Verification:
- Pushed to test branch, opened PR #123
- Workflow triggered automatically: ✅
- All jobs passed (lint: 0 errors, test: 69/69, build: success)
- Total run time: 2m 34s"
```

### Refactoring

```bash
dex complete def123 --result "Refactored storage to one file per task:

Implementation:
- Split tasks.json into .dex/tasks/{id}.json files
- Added auto-migration from old format

Verification:
- All 60 tests passing (including 8 storage tests)
- Build successful
- Manually tested migration: old → new format ✅
- Confirmed git diff shows only changed tasks"
```
