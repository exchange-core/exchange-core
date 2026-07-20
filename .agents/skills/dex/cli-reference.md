# CLI Reference

Full command reference for dex. For basic usage, see SKILL.md.

## Create a Task

```bash
dex create "Task name" --description "Full implementation details"
```

Options:

- `<name>` or `-n, --name`: One-line summary (required)
- `-d, --description`: Full implementation details (optional but recommended)
- `-p, --priority <n>`: Lower = higher priority (default: 1)
- `-b, --blocked-by <ids>`: Comma-separated task IDs that must complete first
- `--parent <id>`: Parent task ID (creates subtask)

## List Tasks

```bash
dex list                      # Show pending tasks (default)
dex list --all                # Include completed
dex list --completed          # Only completed
dex list --ready              # Only tasks ready to work on (no blockers)
dex list --blocked            # Only blocked tasks
dex list --query "login"      # Search in name/description
```

Blocked tasks show an indicator: `[B: xyz123]` (blocked by task xyz123) or `[B: 2]` (blocked by 2 tasks).

## View Task Details

```bash
dex show <id>
```

## Complete a Task

```bash
dex complete <id> --result "What was accomplished" --commit <sha>
dex complete <id> --result "No code changes needed" --no-commit
```

**For GitHub/Shortcut-linked tasks**, you must specify either:

- `--commit <sha>` - Links the commit; issue closes when commit is merged to remote
- `--no-commit` - Completes without a commit; issue stays open (close manually)

Tasks without remote links (no GitHub/Shortcut metadata) don't require either flag.

### Linking Commits

When completing a task that involved creating a commit, link it:

```bash
dex complete abc123 --result "Implemented feature X" --commit a1b2c3d
```

This captures commit SHA, message, and branch automatically. The linked GitHub/Shortcut issue will be closed **only when the commit is pushed to the remote**.

**GitHub Issue References**: If the task is linked to a GitHub issue (visible in `dex show` output), include issue references in your commit message. Use `Fixes #N` for root tasks (closes the issue) or `Refs #N` for subtasks (links without closing).

## Edit a Task

```bash
dex edit <id> -n "Updated name" --description "Updated description"
dex edit <id> --add-blocker xyz123      # Add blocking dependency
dex edit <id> --remove-blocker xyz123   # Remove blocking dependency
```

## Delete a Task

```bash
dex delete <id>
```

Note: Deleting a parent task also deletes all its subtasks.

## Blocking Dependencies

Use blocking dependencies to enforce task ordering:

```bash
# Create a task that depends on another
dex create "Deploy to production" --description "..." --blocked-by abc123

# Add a blocker to an existing task
dex edit xyz789 --add-blocker abc123

# Remove a blocker
dex edit xyz789 --remove-blocker abc123
```

### When to Use Blockers

Use blockers when:

- Task B cannot start until Task A completes
- Multiple tasks depend on a shared prerequisite
- You want to prevent out-of-order completion

Don't use blockers when:

- Tasks can be worked on in parallel
- The dependency is just a logical grouping (use subtasks instead)

### Viewing Blocking Relationships

- `dex list` shows blocked indicator: `[B: xyz123]` or `[B: 2]`
- `dex list --blocked` shows only blocked tasks
- `dex list --ready` shows only tasks with no blockers
- `dex show <id>` displays "Blocked by:" and "Blocks:" sections

Blocked tasks can still be completed (soft enforcement), but you'll see a warning.

## Storage

Tasks are stored as individual files:

- `<git-root>/.dex/tasks/{id}.json` (if in a git repo)
- `~/.dex/tasks/{id}.json` (fallback)

Override with `--storage-path` or `DEX_STORAGE_PATH` env var.

### Task File Format

```json
{
  "id": "abc123",
  "parent_id": null,
  "name": "One-line summary",
  "description": "Full implementation details...",
  "priority": 1,
  "completed": false,
  "result": null,
  "blocked_by": ["xyz789"],
  "created_at": "2026-01-01T00:00:00.000Z",
  "updated_at": "2026-01-01T00:00:00.000Z",
  "completed_at": null
}
```
