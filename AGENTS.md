# Project Agent Guide

This repository is a reusable Codex template for full-stack projects. Treat this
file as the default contract. Prefer a more specific nested `AGENTS.md` when one
exists in the area being changed.

## Working Agreement

- Read relevant code, configuration, tests, and local instructions before editing.
- Preserve established architecture and naming unless the task requires a change.
- Make the smallest coherent change that fully solves the problem.
- Do not overwrite unrelated or user-authored changes.
- Do not add dependencies, frameworks, or abstractions without a concrete need.
- State assumptions when requirements cannot be inferred from the repository.
- Complete implementation, tests, and verification in the same task when feasible.

## Project Discovery

Before implementation:

1. Inspect the repository tree and package manifests.
2. Identify runtime versions, frameworks, entry points, and test commands.
3. Find the nearest analogous feature and follow its conventions.
4. Trace the complete affected path: UI, API, domain, persistence, background
   work, and external integrations as applicable.
5. Check for nested `AGENTS.md` files and relevant skills in `.codex/skills`.

Never assume a project uses every technology listed below. Apply only the
guidance relevant to the detected stack.

## Architecture

- Organize code around business capabilities and clear ownership boundaries.
- Apply DDD where domain complexity justifies it: use explicit domain language,
  invariants, value objects, aggregates, and application services.
- Do not force DDD layers onto simple CRUD. Complexity must pay for itself.
- Keep domain logic independent from HTTP, UI, database, and vendor SDK details.
- Depend on abstractions at volatile boundaries, not around stable one-line code.
- Prefer composition over inheritance. Use inheritance only for a genuine
  substitutable relationship.
- Follow SOLID as a decision tool, not as a class-count target.
- Follow DRY for knowledge and business rules. Do not unify code that merely
  looks similar but changes for different reasons.
- Keep modules cohesive and public interfaces small.
- Make side effects explicit and isolate them at system boundaries.

## Backend: Python

- Follow the supported Python version and existing formatter, linter, and type checker.
- Add type hints to application boundaries and non-trivial business logic.
- Prefer explicit data flow, dependency injection, and small cohesive services.
- Use domain-specific exceptions; translate them at transport boundaries.
- Never expose raw database or infrastructure errors through an API.
- Validate data at trust boundaries. Do not duplicate validation without a
  distinct domain reason.
- Keep sync and async call chains consistent. Never perform blocking I/O in an
  async request path.
- Use timezone-aware UTC datetimes internally.
- Use `Decimal` for money and define rounding rules explicitly.

### FastAPI

- Keep routers thin: parse input, invoke an application service, map output.
- Express dependencies through `Depends`; avoid hidden module-level state.
- Define request and response schemas explicitly and avoid returning ORM models.
- Use appropriate status codes and a consistent error response shape.
- Scope sessions to the request or unit of work.

### Django REST Framework

- Keep serializers focused on transport validation and representation.
- Put reusable business workflows in domain or application services, not viewsets.
- Prevent N+1 queries with deliberate `select_related` and `prefetch_related`.
- Use transactions for multi-write invariants and `on_commit` for post-commit work.
- Keep permissions explicit and test object-level authorization.

### SQLAlchemy 2.x

- Use 2.x-style typed mappings, `select()`, and explicit session boundaries.
- Keep transaction ownership in the application service or unit of work.
- Avoid implicit lazy loading in serialization and async flows.
- Use eager-loading strategies intentionally and verify query counts where relevant.
- Do not call `commit()` inside repositories unless that is the established contract.

## Frontend: React, Next.js, JavaScript, TypeScript

- Prefer function components, hooks, and composition.
- Keep server state, URL state, form state, and local UI state distinct.
- Derive values during render when possible; do not synchronize derived state
  with effects.
- Use effects only for external synchronization and clean them up correctly.
- Keep components focused. Extract logic when it becomes reusable or obscures intent.
- Preserve accessibility: semantic HTML, labels, keyboard operation, focus
  management, and meaningful loading and error states.
- Validate untrusted data at runtime even when TypeScript types exist.
- Avoid `any`; narrow `unknown` and model discriminated states explicitly.
- In Next.js, default to server components and add `"use client"` only where
  browser APIs, state, or interaction require it.
- Do not expose secrets through client bundles or public environment variables.
- Prevent request waterfalls and avoid unnecessary client-side fetching.

## API And Data Contracts

- Treat public APIs, events, and stored data as compatibility boundaries.
- Prefer additive changes. Document and test intentional breaking changes.
- Define pagination, filtering, ordering, nullability, and error semantics.
- Make retried writes idempotent where duplicate execution is possible.
- Validate authorization independently from whether an object exists.

## Database Changes

- Use the repository migration tool; never edit production schemas manually.
- Design migrations for expected data volume and lock behavior.
- Prefer expand/migrate/contract for incompatible or zero-downtime changes.
- Add constraints and indexes intentionally and verify their operational cost.
- Include rollback or forward-fix reasoning for destructive changes.
- Never place secrets or sensitive personal data in fixtures, logs, or migrations.

## Security

- Apply least privilege and deny by default.
- Enforce authentication and authorization server-side for every protected action.
- Prevent injection with parameterized queries and framework-safe APIs.
- Protect cookies, sessions, CORS, CSRF, redirects, uploads, and outbound URLs
  according to the application threat model.
- Never log credentials, tokens, secrets, or unnecessary personal data.
- Keep secret values out of source control and client-side code.
- Treat dependency changes as security-sensitive and review lockfile diffs.

## Testing

- Test behavior at the lowest level that gives confidence.
- Add a regression test before or with every bug fix when practical.
- Cover the happy path, important failure paths, authorization, and boundaries.
- Prefer deterministic tests. Freeze time, seed randomness, and mock only external
  boundaries or genuinely expensive dependencies.
- Use integration tests for database mappings, transactions, framework wiring,
  and API contracts.
- For frontend work, prioritize user-observable behavior over implementation details.
- Do not weaken or delete tests merely to make a change pass.

## Verification

Run the repository's own commands. When available, verify:

1. Focused tests for the changed behavior.
2. The broader relevant test suite.
3. Formatting and linting.
4. Static type checking.
5. Build or migration validation when affected.

Report commands that were not run and the reason.

## Skill Selection

- Use `$backend-feature` for FastAPI, DRF, Python service, and API work.
- Use `$frontend-feature` for React, Next.js, JavaScript, and TypeScript work.
- Use `$database-change` for schema, migration, query, and persistence changes.
- Use `$bug-fix` for reproducing and correcting defects.
- Use `$code-review` for review-only requests.
- Use `$security-review` for threat-focused audits and sensitive changes.

Use multiple skills when the task crosses boundaries, but keep one primary workflow.

## Definition Of Done

- The requested behavior is implemented without unrelated refactoring.
- Architecture and public contracts remain coherent.
- Relevant tests are added or updated and pass.
- Formatting, linting, typing, and builds pass where configured.
- Security, performance, migrations, and compatibility were considered.
- Documentation or examples are updated when behavior or setup changed.
- The final report summarizes changes, verification, and residual risks.
