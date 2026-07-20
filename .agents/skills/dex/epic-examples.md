# Epic Examples

Real examples of well-structured epics from the dex project.

## What Makes a Great Epic

1. **Clear problem statement**: Why this work is needed
2. **Solution overview**: High-level approach
3. **Key design decisions**: Technical choices with rationale
4. **Phased subtasks**: 3-7 tasks, logically ordered
5. **Success criteria**: How to know when it's done

## Example 1: Migrate to JSONL Storage Format

A completed epic showing a focused technical migration.

### Context

```markdown
# Plan: Migrate to JSONL Storage Format

## Summary

Migrate from individual `.dex/tasks/{id}.json` files to a single `tasks.jsonl`
file to reduce file system noise while maintaining merge-safety.

## Problem Statement

- Dozens/hundreds of individual task files create noise
- Directory operations slow with many files
- Git conflicts when multiple developers add tasks

## Solution Overview

JSONL format: one task per line, merge-safe (adding = appending a line).

## Implementation Plan

### Phase 1: Create JSONL Storage Implementation

Create `JsonlStorage` class: parse lines as JSON, atomic writes via temp+rename.

### Phase 2: Add Migration from File-per-Task Format

Auto-detect old format, migrate to JSONL, backup old directory.

### Phase 3: Switch Default Storage

Use JsonlStorage by default, keep FileStorage for compatibility.

### Phase 4: Add Tests

Round-trip, migration, and corruption handling tests.

### Phase 5: Documentation Updates

## Success Criteria

- All existing tests pass
- Migration from old formats works automatically
- No data loss during migration
```

### Subtask Breakdown

```
[x] uv7izxz0: Migrate to JSONL Storage Format (5 subtasks)
├── [x] ocjsepij: Create JSONL Storage Implementation
├── [x] i5or9r1e: Add migration from file-per-task format
├── [x] 0anx9dfc: Switch default storage to JSONL
├── [x] ak51x8dt: Add comprehensive tests for JSONL storage
└── [x] ao7gpv85: Update documentation for JSONL storage format
```

**Why it works**: Focused scope, clear rationale, phased approach with 5 right-sized subtasks.

---

## Example 2: Archive Strategy

An in-progress epic with clear design decisions.

### Context

```markdown
# Archive Strategy: Compacted Task History

## Problem Statement

Completed tasks accumulate over time, consuming memory, slowing performance,
and creating noise in active task lists.

## Solution Overview

Move old completed tasks to `archive.jsonl` in compacted format (dropping
context field for 50-80% size reduction).

## Key Design Decisions

- **Compacted fields**: Archive drops context, blockedBy, blocks
- **Rolled-up children**: Epic's children stored in archived_children array
- **Criteria**: Archive if completed >90 days AND not in recent 50
- **Query behavior**: `dex list` reads only active; `dex show` checks both

## Implementation Plan

### Phase 1: Storage Layer

Create archive storage with compaction logic.

### Phase 2: Manual Archive Command

`dex archive <task-id>` archives task + descendants.

### Phase 3: Auto-Archive

Time + count based archival of complete lineages.

### Phase 4: CLI Integration

Add --archived flag to list, integrate into show/query.

### Phase 5: Tests
```

### Subtask Breakdown

```
[ ] t64hfub3: Archive Strategy (7 subtasks)
├── [x] qi9iakzt: Create Archive Storage Layer
├── [x] qzx9knp8: Implement Compaction Logic
├── [ ] 2uvg80ks: Add manual archive command
├── [ ] j8osnde9: Implement auto-archive functionality
├── [ ] kotl63oy: Add CLI flags for archive operations
├── [ ] xe0pfmym: Integrate archive into query operations
└── [ ] v771l2ru: Add comprehensive archive tests
```

**Why it works**: Problem-first, concrete data formats, key decisions documented, logical task ordering.

---

## Example 3: Dex TUI - Planning Session Interface

A larger epic with three-level hierarchy (Epic -> Phase -> Subtask).

### Context (Abbreviated)

```markdown
# Plan: Dex TUI - Planning Session Interface

## Summary

Terminal UI for managing dex tasks and spawning Claude Code planning sessions.

## Problem Statement

Users need to capture ideas quickly, spawn planning sessions, maintain backlog
visibility, and convert planning outputs into tasks.

## Solution Overview

Ink-based TUI showing task backlog, with keyboard shortcuts for task creation
and planning session management.

## Key Design Decisions

- **UI Framework**: Ink (declarative React patterns for terminal)
- **State**: Zustand (lightweight, persistent)
- **Claude Integration**: claude-agent-sdk

## Implementation Plan

### Phase 1: Core TUI

Task backlog view, keyboard navigation, quick task creation.

### Phase 2: Planning Sessions

Spawn sessions, status sidebar, convert results to tasks.

### Phase 3: Advanced

Templates, batch operations, cost tracking, session resume.
```

### Subtask Breakdown

```
[ ] 85wau5dl: Dex TUI (3 phases)
├── [ ] zxw01uwg: Phase 1: Foundation (6 subtasks)
│   ├── [ ] dac0b3nh: Set up dex-tui package
│   ├── [ ] kgirg52i: Implement basic Ink app
│   └── ... (4 more)
├── [ ] i8855dwx: Phase 2: Planning Sessions (6 subtasks)
└── [ ] 12azrz12: Phase 3: Polish & Advanced (5 subtasks)
```

**Why it works**: Three-level hierarchy for large scope, technology decisions explicit, each phase independently valuable.

---

## Anti-Patterns to Avoid

| Anti-Pattern         | Example                | What's Missing                       |
| -------------------- | ---------------------- | ------------------------------------ |
| Too Vague            | "Improve performance"  | Which operations? What's acceptable? |
| Over-Decomposed      | 15+ subtasks           | Group into 3-5 phases instead        |
| No Problem Statement | "Add Redis caching"    | Why Redis? What problem?             |
| No Success Criteria  | "Refactor auth system" | What does "done" look like?          |

---

## Template

```markdown
# Plan: [Epic Name]

## Summary

One paragraph describing what this epic accomplishes.

## Problem Statement

What pain points does this address?

## Solution Overview

High-level approach (2-3 sentences).

## Key Design Decisions

- Decision 1: Choice and rationale
- Decision 2: Choice and rationale

## Implementation Plan

### Phase 1: [Name]

### Phase 2: [Name]

## Success Criteria

- [ ] Measurable outcome 1
- [ ] Measurable outcome 2
```
