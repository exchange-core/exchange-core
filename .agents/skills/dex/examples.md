# Examples

Good and bad examples for writing task descriptions and results.

## Writing Descriptions

Descriptions should include everything needed to do the work without asking questions:

- **What** needs to be done and why
- **Implementation approach** (steps, files to modify, technical choices)
- **Done when** (acceptance criteria)

### Good Description Example

```bash
dex create "Migrate storage to one file per task" \
  --description "Change storage format for git-friendliness:

Structure:
.dex/
└── tasks/
    ├── abc123.json
    └── def456.json

NO INDEX - just scan task files. For typical task counts (<100), this is fast.

Implementation:
1. Update storage.ts:
   - read(): Scan .dex/tasks/*.json, parse each, return TaskStore
   - write(task): Write single task to .dex/tasks/{id}.json
   - delete(id): Remove .dex/tasks/{id}.json
   - Add readTask(id) for single task lookup

2. Task file format: Same as current Task schema (one task per file)

3. Migration: On read, if old tasks.json exists, migrate to new format

4. Update tests

Benefits:
- Create = new file (never conflicts)
- Update = single file change
- Delete = remove file
- No index to maintain or conflict
- git diff shows exactly which tasks changed"
```

Notice: States the goal, shows the structure, lists specific implementation steps, and explains benefits. Someone could pick this up without asking questions.

### Bad Description Example

```bash
dex create "Add auth" --description "Need to add authentication"
```

❌ Missing: How to implement it, what files, what's done when, technical approach

## Writing Results

Results should capture what was actually done:

- **What changed** (implementation summary)
- **Key decisions** (and why)
- **Verification** (tests passing, manual testing done)

### Good Result Example

```bash
dex complete abc123 --result "Migrated storage from single tasks.json to one file per task:

Structure:
- Each task stored as .dex/tasks/{id}.json
- No index file (avoids merge conflicts)
- Directory scanned on read to build task list

Implementation:
- Modified Storage.read() to scan .dex/tasks/ directory
- Modified Storage.write() to write/delete individual task files
- Auto-migration from old single-file format on first read
- Atomic writes using temp file + rename pattern

Trade-offs:
- Slightly slower reads (must scan directory + parse each file)
- Acceptable since task count is typically small (<100)
- Better git history - each task change is isolated

All 60 tests passing, build successful."
```

Notice: States what changed, lists implementation details, explains trade-offs, confirms verification.

### Bad Result Example

```bash
dex complete abc123 --result "Fixed the storage issue"
```

❌ Missing: What was actually implemented, how, what decisions were made

## Subtask Description Example

Link subtasks to their parent and explain what this piece does specifically:

```
Part of auth system (parent: abc123). This subtask: JWT verification middleware.

What it does:
- Verify JWT signature and expiration on protected routes
- Extract user ID from token payload
- Attach user object to request
- Return 401 for invalid/expired tokens

Implementation:
- Create src/middleware/verify-token.ts
- Export verifyToken middleware function
- Use jsonwebtoken library
- Handle expired vs invalid token cases separately

Done when:
- Middleware function complete and working
- Unit tests cover valid/invalid/expired scenarios
- Integrated into auth routes in server.ts
- Parent task can use this to protect endpoints
```
