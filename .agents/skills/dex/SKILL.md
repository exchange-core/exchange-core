---
name: dex
description: Manage tasks via dex CLI. Use when breaking down complex work, tracking implementation items, or persisting context across sessions.
---

# Agent Coordination with dex

## Command Invocation

Use `dex` directly for all commands. If not on PATH, use `npx @zeeg/dex` instead.

```bash
command -v dex &>/dev/null && echo "use: dex" || echo "use: npx @zeeg/dex"
```

## Core Principle: Tickets, Not Todos

Dex tasks are **tickets** - structured artifacts with comprehensive context:

- **Name**: One-line summary (issue title)
- **Description**: Full background, requirements, approach (issue body)
- **Result**: Implementation details, decisions, outcomes (PR description)

Think: "Would someone understand the what, why, and how from this task alone?"

## Dex Tasks are Ephemeral

**Never reference dex task IDs in external artifacts** (commits, PRs, docs). Task IDs like `abc123` become meaningless once tasks are completed. Describe the work itself, not the task that tracked it.

## When to Use dex

**Use dex when:**

- Breaking down complexity into subtasks
- Work spans multiple sessions
- Context needs to persist for handoffs
- Recording decisions for future reference

**Skip dex when:**

- Work is a single atomic action
- Everything fits in one session with no follow-up
- Overhead exceeds value

## dex vs Built-in Task Tools

Some AI agents (like Claude Code) have built-in task tools. These are session-only and not the same as dex.

|                 | dex                                   | Built-in Task Tools |
| --------------- | ------------------------------------- | ------------------- |
| **Persistence** | Files in `.dex/`                      | Session-only        |
| **Context**     | Rich (description + context + result) | Basic               |
| **Hierarchy**   | 3-level (epic → task → subtask)       | Flat                |

Use **dex** for persistent work. Use built-in task tools for ephemeral in-session tracking only.

## Basic Workflow

### Create a Task

```bash
dex create "Short name" --description "Full implementation context"
```

Description should include: what needs to be done, why, implementation approach, and acceptance criteria. See [examples.md](examples.md) for good/bad examples.

### List and View Tasks

```bash
dex list                  # Pending tasks
dex list --ready          # Unblocked tasks
dex show <id>             # Full details
```

### Complete a Task

```bash
dex complete <id> --result "What was accomplished" --commit <sha>
```

**GitHub/Shortcut-linked tasks** require either `--commit <sha>` or `--no-commit`:

- Use `--commit <sha>` when you have code changes (issue closes when merged)
- Use `--no-commit` for non-code tasks like planning or design (issue stays open)

**Always verify before completing.** Results must include evidence: test counts, build status, manual testing outcomes. See [verification.md](verification.md) for the full checklist.

### Edit and Delete

```bash
dex edit <id> --description "Updated description"
dex delete <id>
```

For full CLI reference including blockers, see [cli-reference.md](cli-reference.md).

## Understanding Task Fields

Tasks have two text fields:

- **Name**: Brief one-line summary (shown in `dex list`)
- **Description**: Full details - requirements, approach, acceptance criteria (shown with `--full`)

When you run `dex show <id>`, the description may be truncated. The CLI will hint at `--full` if there's more content.

### Gathering Context

When picking up a task, gather all relevant context:

```bash
dex show <id> --full              # Full task details
dex show <parent-id> --full       # Parent context (if applicable)
dex show <blocker-id> --full      # What blockers accomplished
```

Before starting, verify you can answer:

- **What** needs to be done specifically?
- **Why** is this needed?
- **How** should it be implemented?
- **When** is it done (acceptance criteria)?

If any answer is unclear:

1. Check parent task or completed blockers for more details
2. Suggest entering plan mode to flesh out requirements before starting

**Proceed without full context when:**

- Task is trivial/atomic (e.g., "Add .gitignore entry")
- Conversation already provides the missing context
- Description itself is sufficiently detailed

## Task Hierarchies

Three levels: **Epic** (large initiative) → **Task** (significant work) → **Subtask** (atomic step).

**Choosing the right level:**

- Small feature (1-2 files) → Single task
- Medium feature (3-7 steps) → Task with subtasks
- Large initiative (5+ tasks) → Epic with tasks

```bash
# Create subtask under parent
dex create --parent <id> "Subtask name" --description "..."
```

For detailed hierarchy guidance, see [hierarchies.md](hierarchies.md).

## Recording Results

Complete tasks **immediately after implementing AND verifying**:

- Capture decisions while fresh
- Note deviations from plan
- Document verification performed
- Create follow-up tasks for tech debt

Your result must include explicit verification evidence. Don't just describe what you did—prove it works. See [verification.md](verification.md).

## Commit Messages with GitHub Issues

When a task is linked to a GitHub issue (shown in `dex show` output), include issue references in commit messages:

- **Root tasks** (the task itself has GitHub metadata): Use `Fixes #N`
  - This closes the issue when merged
- **Subtasks** (parent/ancestor has GitHub metadata): Use `Refs #N`
  - This links to the issue without closing it

Check `dex show <id>` for GitHub issue info before committing. The "(via parent)" indicator means use `Refs`, direct metadata means use `Fixes`.

## Best Practices

1. **Right-size tasks**: Completable in one focused session
2. **Clear completion criteria**: Description should define "done"
3. **Don't over-decompose**: 3-7 children per parent
4. **Action-oriented descriptions**: Start with verbs ("Add", "Fix", "Update")
5. **Verify before completing**: Tests passing, manual testing done

## Additional Resources

- [cli-reference.md](cli-reference.md) - Full CLI documentation
- [examples.md](examples.md) - Good/bad context and result examples
- [verification.md](verification.md) - Verification checklist and process
- [hierarchies.md](hierarchies.md) - Epic/task/subtask organization
