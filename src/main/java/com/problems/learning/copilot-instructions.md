# Copilot Agent Repo Guidance

**Source of truth**
- Tasks live in `tasks.md` (Backlog → In Progress → Done).
- Project conventions live in `Claude.md`.

**Always do**
1. Read `tasks.md` (Backlog) and pick the first task unless told otherwise.
2. For each task:
   - Follow its **Scope**, **Steps**, **Acceptance**.
   - Edit all referenced files (they may not be open).
   - Update `tasks.md`: tick only finished items; move card to **In Progress** or **Done**.
   - If you create new conventions, update `Claude.md`.

**Editing rules**
- Keep diffs minimal and focused on the task.
- If a task is too large for one run, complete **some Steps** and leave a short **Notes** line with what’s left.

**PR/Commit**
- One PR per task.
- PR title: `<type>: <short task title>` (e.g., `feat: add retry policy to Kafka consumer`)
- PR body:
  - Summary of changes
  - Checklist copied from **Acceptance** with boxes ticked
  - Any follow-ups (bullets)

**Build/Test**
- Build: `./gradlew clean build`
- Tests: `./gradlew test`
- Lint/Format: `./gradlew spotlessApply` (update if different)

**Style**
- Java 17, SLF4J logging (INFO for outcomes, DEBUG for internals).
- No broad refactors unless the task says so.

**Failure handling**
- If blocked (missing dep/secret), write a **Notes** line in `tasks.md` and open the PR with what’s done.