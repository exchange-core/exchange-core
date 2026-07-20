# Task Hierarchies

Guidance for organizing work into epics, tasks, and subtasks.

## Three Levels

| Level | Name        | Purpose                     | Example                           |
| ----- | ----------- | --------------------------- | --------------------------------- |
| L0    | **Epic**    | Large initiative (5+ tasks) | "Add user authentication system"  |
| L1    | **Task**    | Significant work item       | "Implement JWT middleware"        |
| L2    | **Subtask** | Atomic implementation step  | "Add token verification function" |

**Maximum depth is 3 levels.** Attempting to create a child of a subtask will fail.

## When to Use Each Level

### Single Task (No Hierarchy)

- Small feature (1-2 files, ~1 session)
- Work is atomic, no natural breakdown

### Task with Subtasks

- Medium feature (3-5 files, 3-7 steps)
- Work naturally decomposes into discrete steps
- Subtasks could be worked on independently

### Epic with Tasks

- Large initiative (multiple areas, many sessions)
- Work spans 5+ distinct tasks
- You want high-level progress tracking

## Creating Hierarchies

```bash
# Create the epic
dex create "Add user authentication system" \
  --description "Full auth system with JWT tokens, password reset..."

# Create tasks under it (note the epic ID, e.g., abc123)
dex create --parent abc123 "Implement JWT token generation" \
  --description "Create token service with signing and verification..."

dex create --parent abc123 "Add password reset flow" \
  --description "Email-based password reset with secure tokens..."

# For complex tasks, add subtasks
dex create --parent def456 "Add token verification function" \
  --description "Verify JWT signature and expiration..."
```

## Subtask Best Practices

Each subtask should be:

- **Independently understandable**: Clear on its own
- **Linked to parent**: Reference parent, explain how this piece fits
- **Specific scope**: What this subtask does vs what parent/siblings do
- **Clear completion**: Define "done" for this piece specifically

## Decomposition Strategy

When faced with large tasks:

1. **Assess scope**: Is this epic-level (5+ tasks) or task-level (3-7 subtasks)?
2. Create parent task/epic with overall goal and context
3. Analyze and identify 3-7 logical children
4. Create children with specific contexts and boundaries
5. Work through systematically, completing with results
6. Complete parent with summary of overall implementation

### Don't Over-Decompose

- 3-7 children per parent is usually right
- If you'd only have 1-2 subtasks, just make separate tasks
- If you need L3, restructure your breakdown

## Viewing Hierarchies

```bash
dex list                    # Full tree view with all levels
dex list abc123             # Show epic abc123 and its subtree
dex show abc123             # Epic details with task/subtask counts
dex show def456             # Task details with breadcrumb path
```

## Completion Rules

- A task cannot be completed while it has pending subtasks
- Complete all children before completing the parent
- Parent result should summarize the overall implementation
