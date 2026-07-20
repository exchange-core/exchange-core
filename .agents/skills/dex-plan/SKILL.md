---
name: dex-plan
description: Create dex task from markdown planning documents (plans, specs, design docs, roadmaps)
---

# Converting Markdown Documents to Tasks

## Command Invocation

Use `dex` directly for all commands:

```bash
dex <command>
```

If `dex` is not on PATH, use `npx @zeeg/dex <command>` instead. Check once at the start:

```bash
command -v dex &>/dev/null && echo "use: dex" || echo "use: npx @zeeg/dex"
```

Use `/dex-plan` to convert any markdown planning document into a trackable dex task.

## When to Use

- After completing a plan in plan mode
- Converting specification documents to trackable tasks
- Converting design documents to implementation tasks
- Creating tasks from roadmap or milestone documents
- Tracking any markdown planning or design content

## Supported Documents

Any markdown file containing planning or design content:

- Plan files from plan mode (`~/.claude/plans/*.md`)
- Specification documents (`SPEC.md`, `REQUIREMENTS.md`)
- Design documents (`DESIGN.md`, `ARCHITECTURE.md`)
- Roadmaps and milestone documents (`ROADMAP.md`)
- Feature proposals and technical RFCs

## Usage

```bash
/dex-plan <markdown-file-path>
```

### Examples

**From plan mode:**

```bash
/dex-plan /home/user/.claude/plans/moonlit-brewing-lynx.md
```

**From specification document:**

```bash
/dex-plan @SPEC.md
```

**From design document:**

```bash
/dex-plan docs/AUTHENTICATION_DESIGN.md
```

**From roadmap:**

```bash
/dex-plan ROADMAP.md
```

## What It Does

1. Reads the markdown file
2. Extracts title from first `#` heading (or uses filename as fallback)
3. Strips "Plan: " prefix if present (case-insensitive)
4. Creates dex task with full markdown content as context
5. Analyzes plan structure for potential subtask breakdown
6. Automatically creates subtasks when appropriate
7. Returns task ID and breakdown summary

## Examples

**From plan mode file:**

```markdown
# Plan: Add JWT Authentication

## Summary

...
```

→ Task description: "Add JWT Authentication" (note: "Plan: " prefix stripped)

**From specification document:**

```markdown
# User Authentication Specification

## Requirements

...
```

→ Task description: "User Authentication Specification"

## Automatic Subtask Breakdown

After creating the main task, the skill analyzes the plan structure to determine if breaking it into subtasks adds value.

### Hierarchy Levels

The skill supports up to 3 levels (maximum depth enforced by dex):

| Level | Name        | Example                           |
| ----- | ----------- | --------------------------------- |
| L0    | **Epic**    | "Add user authentication system"  |
| L1    | **Task**    | "Implement JWT middleware"        |
| L2    | **Subtask** | "Add token verification function" |

### When Breakdown Happens

The skill creates subtasks when the plan has:

- 3-7 clearly separable work items (numbered steps, distinct sections, implementation phases)
- Implementation across multiple files or components (different modules, layers, or areas)
- Clear sequential dependencies (step 1 before step 2)
- Independent items that benefit from separate tracking

**Epic-level breakdown** (creates tasks, not subtasks) when:

- Plan has major phases/sections with their own sub-items
- 5+ distinct areas of work
- Plan spans multiple systems or components
- Work will require multiple sessions

### When Breakdown Does NOT Happen

The skill keeps a single task when:

- Plan describes one cohesive task (even if detailed with multiple paragraphs)
- Only 1-2 steps present (not enough to warrant breakdown)
- Work items are tightly coupled (can't be separated meaningfully)
- Plan is exploratory or investigative (research, analysis, discovery)
- Breaking down would create artificial boundaries that don't reflect natural work units

### What Each Subtask Contains

When breakdown occurs, each subtask includes:

- Description: Brief summary extracted from list item, heading, or section
- Context: Relevant details from that section plus reference to parent task
- Parent link: Automatically linked to main task via `--parent`

### Example: With Breakdown

**Input plan** (`auth-plan.md`):

```markdown
# Plan: Add Authentication System

## Implementation

1. Create database schema for users/tokens
2. Implement auth controller with endpoints
3. Add JWT middleware for route protection
4. Build frontend login/register forms
5. Add integration tests
```

**Output**:

```
Created task abc123 from plan

Analyzed plan structure: Found 5 distinct implementation steps
Created 5 subtasks:
- abc124: Create database schema for users/tokens
- abc125: Implement auth controller with endpoints
- abc126: Add JWT middleware for route protection
- abc127: Build frontend login/register forms
- abc128: Add integration tests

View full structure: dex show abc123
```

### Example: Without Breakdown

**Input plan** (`bugfix-plan.md`):

```markdown
# Plan: Fix Login Validation Bug

## Problem

Login fails when username has spaces

## Solution

Update validation regex in auth.ts line 42 to allow spaces
```

**Output**:

```
Created task xyz789 from plan

Plan describes a cohesive single task. No subtask breakdown needed.

View task: dex show xyz789
```

### Example: Epic-Level Breakdown (Two-Level Hierarchy)

**Input plan** (`full-auth-plan.md`):

```markdown
# Plan: Complete User Authentication System

## Phase 1: Backend Infrastructure

1. Create database schema for users and sessions
2. Implement password hashing with bcrypt
3. Add JWT token generation and validation

## Phase 2: API Endpoints

1. POST /auth/register - User registration
2. POST /auth/login - User login
3. POST /auth/logout - Session invalidation
4. POST /auth/reset-password - Password reset flow

## Phase 3: Frontend Integration

1. Login/register forms with validation
2. Protected route components
3. Session persistence with refresh tokens
```

**Output**:

```
Created epic abc123 from plan

Analyzed plan structure: Found 3 major phases with sub-items
Created as epic with 3 tasks:
- def456: Backend Infrastructure (3 subtasks)
- ghi789: API Endpoints (4 subtasks)
- jkl012: Frontend Integration (3 subtasks)

View full structure: dex list abc123
```

## Options

```bash
/dex-plan <file> --priority 2              # Set priority
/dex-plan <file> --parent abc123           # Create as subtask
```

## After Creating

Once created, you can:

- View the task: `dex show <task-id>`
- Create additional subtasks: `dex create "..." --parent <task-id> --description "..."`
- Track progress through implementation
- Complete the task: `dex complete <task-id> --result "..."`

Run `dex show <task-id>` to see the full task structure including any automatically created subtasks.

## When NOT to Use

- Document is incomplete or exploratory (just draft notes)
- Content isn't actionable or ready for implementation
- File hasn't been saved to disk yet
- File doesn't contain meaningful planning/design content

---

## Implementation Instructions for Skill

**These instructions are for the skill agent executing `/dex-plan`.** Follow this workflow exactly:

### Step 1: Create Main Task

Execute the `dex plan` command with the provided markdown file:

```bash
dex plan <markdown-file> [options]
```

This creates the parent task and returns its ID. Capture this ID for subsequent steps.

### Step 2: Read and Analyze the Plan

After creating the main task, read it back to analyze its structure:

```bash
dex show <task-id>
```

Examine the context field (which contains the full markdown) for breakdown potential.

### Step 3: Apply Breakdown Decision Logic

**Analyze the plan structure and decide**: Should this be broken down into subtasks?

#### Look for these breakdown indicators:

1. Numbered or bulleted implementation lists (3-7 items):

   ```markdown
   ## Implementation

   1. Create database schema → SUBTASK
   2. Build API endpoints → SUBTASK
   3. Add frontend components → SUBTASK
   ```

2. Clear subsections under implementation/tasks/steps:

   ```markdown
   ### 1. Backend Changes

   - Modify server.ts
   - Add authentication
     → SUBTASK: "Backend Changes" with this context

   ### 2. Frontend Updates

   - Update login form
   - Add error handling
     → SUBTASK: "Frontend Updates" with this context
   ```

3. File-specific sections:

   ```markdown
   ### `src/auth.ts` - Add JWT validation

   [Details about changes]
   → SUBTASK: "Add JWT validation to auth.ts"

   ### `src/middleware.ts` - Create auth middleware

   [Details about changes]
   → SUBTASK: "Create auth middleware"
   ```

4. Sequential phases:

   ```markdown
   ## Implementation Sequence

   **Phase 1: Database Layer**
   [Details] → SUBTASK

   **Phase 2: API Layer**
   [Details] → SUBTASK

   **Phase 3: Frontend Layer**
   [Details] → SUBTASK
   ```

#### Do NOT break down when:

- Only 1-2 steps/items present
- Plan is a single cohesive fix or small change
- Content is exploratory ("investigate", "research", "explore")
- Work items are inseparable (tightly coupled implementation)
- Breaking down creates artificial boundaries
- Plan is very short (< 10 lines of meaningful content)

### Step 4: Extract Subtasks (If Breaking Down)

For each identified subtask:

1. Extract description: Use the list item text, heading, or section title
   - Strip numbering and bullets: "1. Add auth" → "Add auth"
   - Keep it concise (1-10 words)
   - Use imperative form: "Add", "Create", "Update", "Fix"

2. Extract context: Include relevant details from that section
   - Copy the full section content for that subtask
   - Add reference: "This is part of [parent task description]"
   - Include code snippets, file paths, specific requirements

3. Create the subtask:
   ```bash
   dex create "<subtask-description>" \
     --parent <parent-task-id> \
     --description "<extracted-context-with-parent-reference>"
   ```

### Step 5: Report Results

If subtasks were created:

```
Created task <id> from plan

Analyzed plan structure: Found <N> distinct implementation steps
Created <N> subtasks:
- <subtask-id-1>: <description-1>
- <subtask-id-2>: <description-2>
- <subtask-id-3>: <description-3>
...

View full structure: dex show <parent-id>
```

If no breakdown occurred:

```
Created task <id> from plan

Plan describes a cohesive single task. No subtask breakdown needed.

View task: dex show <id>
```

### Examples of Subtask Extraction

Example 1: Numbered list

```markdown
## Implementation Steps

1. Create User model with email, password fields
2. Add POST /api/auth/register endpoint
3. Implement JWT token generation
```

Extracted subtasks:

```bash
dex create "Create User model with email, password fields" \
  --parent abc123 \
  --description "Create a User model with email and password fields. This is part of 'Add Authentication System'."

dex create "Add POST /api/auth/register endpoint" \
  --parent abc123 \
  --description "Add POST /api/auth/register endpoint to handle user registration. This is part of 'Add Authentication System'."

dex create "Implement JWT token generation" \
  --parent abc123 \
  --description "Implement JWT token generation for authenticated sessions. This is part of 'Add Authentication System'."
```

Example 2: Subsections with details

```markdown
### Frontend: Login Form Component

Create a new React component at `src/components/LoginForm.tsx`:

- Email and password inputs
- Submit button with loading state
- Error message display
- Validation on submit

### Backend: Auth Routes

Add to `src/routes/auth.ts`:

- POST /login endpoint
- Password verification using bcrypt
- JWT token generation on success
```

Extracted subtasks:

```bash
dex create "Frontend: Login Form Component" \
  --parent abc123 \
  --description "Create a new React component at src/components/LoginForm.tsx with email/password inputs, submit button with loading state, error message display, and validation on submit. This is part of 'Add Authentication System'."

dex create "Backend: Auth Routes" \
  --parent abc123 \
  --description "Add to src/routes/auth.ts: POST /login endpoint, password verification using bcrypt, JWT token generation on success. This is part of 'Add Authentication System'."
```

Example 3: Should NOT break down

```markdown
# Plan: Fix Typo in Error Message

## Problem

Error message says 'Sucessful' instead of 'Successful'

## Solution

Fix typo in src/messages.ts line 42
```

Decision: Single cohesive task, only one change. Do NOT create subtasks.

### Key Principles

1. Agent judgment is critical: Use intelligence to determine if breakdown adds value
2. Err on the side of NOT breaking down: Only break down when it clearly helps
3. Each subtask must be meaningful: Not just a single line change
4. Context is essential: Each subtask should have enough context to be actionable independently
5. Preserve plan semantics: Don't force a structure that doesn't match the plan's intent
