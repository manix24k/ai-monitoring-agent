# Superlog Understanding for Agentic Code-Fix PR Automation

This document captures how Superlog currently implements its incident-to-agent-to-PR workflow, and which parts are directly reusable for the next phase of `ai-monitoring-agent` (high-intelligence agentic code-fix PR creation).

## Snapshot Analyzed

- Repository: `superloglabs/superlog`
- Local path analyzed: `/Users/administrator/Documents/superlog`
- Branch: `main`
- Head commit: `ef9ec92ec3635b1e7165406a095140706a4c8e10`
- Monorepo layout: `apps/web`, `apps/api`, `apps/proxy`, `apps/worker`, `packages/db`, `packages/fingerprint`

## What Superlog Is Doing Well (Relevant to Our Use Case)

### 1) Explicit state machine for agent runs

Superlog treats agent execution as a governed lifecycle, not a single fire-and-forget call. Core states include `queued`, `repo_discovery`, `running`, `awaiting_human`, `pr_retry_queued`, `complete`, and multiple failure/blocked paths.

Why this matters for us:

- Gives deterministic behavior for retries and recovery.
- Prevents silent stalls.
- Makes UI and audit logs straightforward.

Reference points:

- `apps/worker/src/agent-run.ts`
- `apps/worker/src/agent-runs/tick.ts`
- `packages/db/src/schema.ts` (`agent_runs`, `incident_events`)

### 2) Tick-based orchestration with fairness and starvation control

`tickAgentRuns()` processes active runs oldest-stalest first (`asc(updatedAt)`), and touches `updatedAt` before each handler so one problematic run cannot monopolize the queue.

Why this matters for us:

- Similar to our Jenkins auto queue concerns: fairness and non-blocking progression.
- A robust pattern for serial workers (`max_workers=1`) and for future concurrent workers.

Reference:

- `apps/worker/src/agent-runs/tick.ts`

### 3) Strong pre-flight repo discovery + ranking

Before starting a managed session, Superlog:

- Verifies GitHub installation presence.
- Enumerates accessible repositories.
- Filters by grant scope and disabled repos.
- Scores repos using incident service tokens and normalized stack frame tokens.
- Limits candidates by backend capacity (`maxRepoResources`).

Why this matters for us:

- Our current Jenkins job resolution gaps (service->pipeline mapping failures) can use the same candidate ranking mindset.
- Better than brittle one-to-one static mapping.

Reference:

- `apps/worker/src/agent-run-context.ts`
- `apps/worker/src/agent-runs/start.ts`

### 4) Defensive normalization of agent outputs

Superlog explicitly validates and normalizes managed-agent result payloads before storing them. Required fields must validate; optional structured fields are dropped if malformed.

Why this matters for us:

- Avoids UI/runtime crashes from malformed LLM outputs.
- Lets automation continue with partial but safe data.

Reference:

- `apps/worker/src/managed-agent-result.ts`

### 5) PR delivery as a separate, resilient stage

PR creation is separated from "analysis complete":

- Patch retrieval and normalization is explicit.
- Git operations are hardened (credential-safe env usage, redaction, retry behavior).
- Failures are categorized (`pr_open_failed`, `patch_validation_failed`, etc.) and surfaced.
- Retry path exists (`pr_retry_queued`) without losing the patch.

Why this matters for us:

- Mirrors our desired "auto-first + manual exact retry" behavior.
- Supports deterministic re-delivery after transient or policy failures.

Reference:

- `apps/worker/src/agent-runs/pr-delivery.ts`
- `apps/worker/src/github-app.ts`

### 6) Rich event/audit model

Every important transition is recorded in `incident_events` with dedupe semantics, parentage constraints, and processed markers. This cleanly supports timeline views, webhook payloads, and retry-safe integrations.

Why this matters for us:

- We should keep explicit per-service run events in our dashboard APIs, not inferred transient state only.
- Makes operator trust and debugging much easier.

Reference:

- `packages/db/src/schema.ts` (`incident_events`)
- `docs/webhooks.md` (`agent_run.completed` semantics)

## End-to-End Superlog Flow (Condensed)

1. Telemetry is ingested and grouped into issues/incidents.
2. Worker tick picks active/queued agent runs fairly.
3. Repo discovery selects candidate repos and starts agent session.
4. Sync loop ingests session events, dispatches tool calls, checks time budgets.
5. Result is validated/normalized and metadata applied.
6. If PR is pending and policy allows, PR delivery runs (patch apply/push/open).
7. Outcome is persisted to tables (`agent_runs`, `agent_pull_requests`, events).
8. Slack/webhook notifications are emitted for operator/integration visibility.

## Directly Adoptable Ideas for `ai-monitoring-agent`

### A) Introduce an explicit "code-fix run" lifecycle for Jenkins+PR automation

Recommended states:

- `queued`
- `resolving_pipeline`
- `dispatching`
- `verifying`
- `awaiting_human` (manual pipeline choice)
- `complete`
- `failed`

Add stable failure reasons (`pipeline_not_found`, `invalid_json_response`, `dispatch_failed`, `verify_timeout`, etc.) and keep them visible in UI/API.

### B) Replace single mapping lookup with scored pipeline candidates

Candidate score inputs:

- service name exact match
- normalized token overlap
- namespace/env match
- historical success (learned overrides)

Use top-N attempts with clear stop conditions and failure reasons.

### C) Keep PR generation/delivery as a separate stage

Do not couple root-cause detection directly to immediate PR open. Persist an intermediate structured result and then perform PR delivery with retry-safe semantics.

### D) Add strict result schema normalization before persistence/UI usage

For any future LLM-generated structured payload (root cause, impact, patch metadata, validation), validate required fields and coerce/drop malformed optional fields.

### E) Persist event stream, not only final status

Store transition events with dedupe keys and timestamps so dashboard and APIs can show exact progression and not lose failure context.

## Practical Target Design for Next Phase (Minimal-Risk)

### Phase 1: Stability-first foundation

- Create `code_fix_runs` + `code_fix_run_events` persistence.
- Add lifecycle transitions in worker logic (no LLM behavior change yet).
- Ensure dashboard surfaces state, reason, and manual-retry actions.

### Phase 2: Candidate resolution intelligence

- Add scored candidate resolver for pipelines/jobs.
- Add fallback chain and explicit reason codes.
- Learn from successful manual `Run w/pipeline` overrides (already partly present).

### Phase 3: Agentic patch/PR engine integration

- Define normalized result contract (analysis, patch metadata, validation result).
- Add PR delivery stage with resilient retries and safe Git auth handling.
- Track PR state transitions in persistent records.

### Phase 4: Operator and integration ecosystem

- Expose webhook events (`code_fix_run.completed`, `code_fix_run.failed`).
- Add structured delivery logs and replay/redelivery controls.
- Keep failure visibility first-class in UI.

## Key Differences vs Our Current System

- Superlog has a first-class DB lifecycle model; ours is currently more runtime-memory/dashboard driven for parts of Jenkins flow.
- Superlog separates "analysis complete" from "PR delivered"; our current system is earlier in that separation.
- Superlog has strongly typed normalized agent result handling; ours should adopt this before scaling agentic behavior.

## Risks to Watch While Adopting

- Over-coupling with existing Jenkins pipeline specifics instead of introducing a generic lifecycle layer.
- Adding LLM autonomy before persistence/event model hardening.
- Missing deterministic failure reason taxonomy (hurts dashboard clarity and auto-recovery).
- Not preserving manual operator controls while increasing automation.

## Concrete File References Used for This Understanding

- `README.md`
- `apps/worker/src/agent-run.ts`
- `apps/worker/src/agent-runs/tick.ts`
- `apps/worker/src/agent-runs/start.ts`
- `apps/worker/src/agent-runs/sync.ts`
- `apps/worker/src/agent-runs/status.ts`
- `apps/worker/src/agent-runs/pr-delivery.ts`
- `apps/worker/src/agent-run-context.ts`
- `apps/worker/src/managed-agent-result.ts`
- `apps/worker/src/github-app.ts`
- `packages/db/src/schema.ts`
- `docs/webhooks.md`

## Bottom Line

For high-intelligence agentic code-fix PR automation, the most valuable pattern from Superlog is not only "generate a patch," but the full operational envelope around it: explicit lifecycle, fairness in orchestration, robust output normalization, resilient delivery, and audit-quality event persistence. That envelope is the right next-step blueprint for our Jenkins + PR automation evolution.
