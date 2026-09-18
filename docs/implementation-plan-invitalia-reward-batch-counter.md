# Implementation Plan: Invitalia Reward-Batch Counter Lifecycle

## Status

Proposed implementation sequence.

This document is the execution plan for
[`invitalia-reward-batch-counter-design-review.md`](invitalia-reward-batch-counter-design-review.md).
The design review remains the authoritative decision record. This document
defines how to implement it through small, human-reviewed changes.

## Purpose

Implement the two lifecycle snapshots and the related live aggregate
projections without asking one coding agent to change the entire reward-batch
domain at once.

The SQL migration baseline described in
[`DR-rework-reward-batches-in-sql.md`](DR-rework-reward-batches-in-sql.md) is
assumed to be already implemented. This plan is therefore an incremental
second wave and must not restart the completed Mongo-to-SQL migration work.

## Execution model

- Use one implementation slice per branch and pull request.
- Use one coding-agent run per slice. Follow-up turns are acceptable inside a
  slice, but start a fresh context for the next slice.
- Do not run agents concurrently against the same lifecycle or persistence
  files.
- Require the agent to inspect the current implementation and existing tests
  before editing.
- Keep the design review immutable; put implementation details and review
  checkpoints here.
- Do not deploy intermediate slices. Each slice is a human review and
  validation checkpoint.
- Merge slices in dependency order and use the latest approved baseline for
  the next slice.

## Shared invariants

Every slice must preserve these rules:

1. `initialAmountCents` is live only in `CREATED`, then is captured exactly on
   entry into `SENT`.
2. `suspendedAmountCents` is live through `EVALUATING`, then is captured
   exactly on entry into `APPROVING`.
3. A captured zero is different from an unset snapshot.
4. A captured snapshot is never overwritten by later reward updates,
   detachment, reassignment, reversal, or invoice replacement.
5. A missing snapshot after its freeze state must fail the read with an
   explicit data-integrity error. Reconciliation may report the condition
   separately, but reads must not silently fall back to a live aggregate.
6. `approvedAmountCents` is zero in `CREATED` and `SENT`; from `EVALUATING`
   onward it sums `TO_CHECK`, `CONSULTABLE`, and `APPROVED` rows.
7. `currentAmountCents` sums all currently assigned transaction rows.
8. `excludedAmountCents` sums currently assigned `REJECTED` rows.
9. All monetary aggregates use the signed typed
   `reward_transactions.accrued_reward_cents` value.
10. `TO_APPROVE` and `TO_WORK` remain query translations and are not persisted
    transaction statuses.
11. Aggregate-affecting writes use the shared batch-locking strategy.
12. A transaction has one initiative and at most one current batch
    membership.
13. Existing endpoints, response semantics, Kafka contracts, payment
    ownership, Blob paths, and external integrations remain compatible.
14. Payment-driven impact handling uses `transactionRevision` as its sole
    ordering and idempotency boundary. Equal or stale revisions are no-ops;
    no impact inbox or second watermark is introduced.

## Implementation slices

### D0 - Produce a read-only gap analysis

**Type:** analysis only; no code changes.

The agent must compare the design review with the current repository and
produce:

- the current implementation of batch projections and lifecycle transitions;
- the files and tests affected by each later slice;
- existing behavior that must remain unchanged;
- conflicts, missing infrastructure, or assumptions requiring human
  decisions;
- a proposed dependency graph for D1-D8.

The agent must not edit files, create migrations, or refactor code.

**Human gate:** approve the gap matrix and confirm that the SQL migration
baseline is the correct starting point.

### D1 - Add lifecycle snapshot storage

**Depends on:** D0.

Add the next ordered PostgreSQL migration, currently expected to be
`V009__add_reward_batch_lifecycle_snapshots.sql`, with:

```text
initial_amount_cents_at_send BIGINT NULL
suspended_amount_cents_at_approving BIGINT NULL
```

Also:

- regenerate jOOQ sources through the repository build;
- update persistence mapping for the raw snapshot columns;
- keep the columns nullable before their transition;
- do not reintroduce mutable operational counters;
- do not add non-negative constraints to signed accrued rewards;
- keep raw snapshot values separate from effective `RewardBatch` projection
  values;
- ensure generic entity saves cannot write effective live values into the raw
  snapshot columns.

Add PostgreSQL migration/schema tests for column names, types, nullability,
and preservation of the existing aggregate indexes and constraints.

**Out of scope:** lifecycle behavior, DTO changes, and query formula changes.

**Human review question:** can the raw snapshot columns be persisted without
allowing a generic batch save to overwrite them?

### D2 - Implement the effective aggregate read model

**Depends on:** D1.

Update the SQL batch projection, model, DTO, and mapper to implement:

| Batch status | `initialAmountCents` | `suspendedAmountCents` |
| --- | --- | --- |
| `CREATED` | Live aggregate | Live aggregate |
| `SENT`, `EVALUATING` | Send snapshot | Live aggregate |
| `APPROVING`, `APPROVED`, `PENDING_REFUND`, `NOT_REFUNDED`, `REFUNDED` | Send snapshot | Approval snapshot |

Add additive `Long` fields to `RewardBatchDTO` and the internal projection:

```text
currentAmountCents
excludedAmountCents
```

The fields must be populated with `0` for an empty aggregate and must not be
omitted because of a missing read-path assignment.

Update database-side expressions used by:

- batch list and detail reads;
- amount sorting;
- approved-batch delivery selection;
- count and status filters that consume the effective aggregates.

A missing frozen snapshot after `SENT` or `APPROVING` must fail the read with
an explicit data-integrity error. Reconciliation may report the same
condition separately, but it must not substitute a live aggregate and the
query must not implement `COALESCE(snapshot, live)` after the freeze state.

Add focused tests for:

- the complete state matrix;
- empty and zero-valued aggregates;
- signed accrued rewards;
- current and excluded amounts;
- the approved-amount state gate;
- fail-closed missing-snapshot errors;
- list/detail mapping consistency;
- amount sorting.

**Out of scope:** writing or capturing snapshots during lifecycle transitions.

**Human review question:** do every read path and sort path use the same
effective definitions?

### D3 - Make `CREATED -> SENT` atomic

**Depends on:** D2.

Replace the merchant-send read/mutate/generic-save flow with a semantic
transactional operation.

The operation must:

1. Open one reactive SQL transaction.
2. Lock the batch row with `SELECT FOR UPDATE` on the transaction-bound
   connection.
3. Validate merchant ownership, `CREATED` status, month chronology, and the
   earlier non-empty `CREATED` batch rule while the lock is held.
4. Calculate the sum of `accrued_reward_cents` over all currently assigned
   transactions.
5. Persist the sum, including `0` for an empty batch, into
   `initial_amount_cents_at_send`.
6. Set `SENT` and the merchant-send timestamp in the same transaction.
7. Guard the mutation by the expected old status and a null snapshot.
8. Return the existing target state and snapshot on a retry after commit.
9. Never overwrite an already captured snapshot.

Adapt the service and port to call this semantic operation. Do not use a
generic full-row save for this transition.

Add reactive unit tests and PostgreSQL integration tests for:

- successful non-empty send;
- empty-batch send;
- merchant and chronology errors;
- previous non-empty batch rejection;
- concurrent assignment/reward update versus send;
- retry after commit;
- missing or inconsistent snapshot behavior.

**Human review question:** can the send path commit a status change without
its snapshot, or capture a snapshot without changing status?

### D4 - Make `EVALUATING -> APPROVING` atomic

**Depends on:** D3.

Replace the current generic status mutation with a semantic approval-entry
operation.

The operation must:

- lock the batch row in a reactive SQL transaction;
- validate the expected `EVALUATING` status, assignee, and existing
  final-approval rules;
- calculate the live sum of assigned `SUSPENDED` transactions;
- persist that sum, including `0`, into
  `suspended_amount_cents_at_approving`;
- set `APPROVING`, approval timestamp, and update timestamp atomically;
- be guarded by the expected old status and a null approval snapshot;
- return the existing state and snapshot on an idempotent retry;
- fail closed with an explicit data-integrity error when a frozen-state
  snapshot is missing; reconciliation may report the condition separately;
- avoid generic full-row save.

Add tests for:

- suspended and non-suspended amounts;
- empty batches;
- signed accrued rewards;
- repeated transition requests;
- transaction decisions racing with approval entry;
- rollback when snapshot persistence or status update fails.

**Human review question:** is the suspended amount frozen exactly on entry
into `APPROVING`, before the final-approval worker moves rows?

### D5 - Align single-batch aggregate-affecting writers

**Depends on:** D3 and D4.

Audit and update all writers that can change the rows used by a batch
aggregate, including:

- invoiced transaction assignment;
- operator transaction decisions;
- reward synchronization that changes accrued reward, membership, or
  in-batch status;
- payment-driven local impact handling;
- any other single-batch membership or amount mutation discovered by D0.

Each operation must:

- use a transaction-bound SQL connection;
- acquire the batch row lock before changing relevant transaction rows;
- preserve initiative and single-membership constraints;
- remain idempotent under retries;
- avoid mutable batch counter updates;
- leave captured snapshots unchanged.

For payment-driven impact handling, use `transactionRevision` as the sole
ordering and idempotency boundary. Equal or stale revisions are no-ops. Do
not introduce an impact inbox or a second watermark.

Where a transaction has no current batch, no batch lock is required until a
membership is established. When a membership exists, the corresponding batch
must be locked before the aggregate-affecting transaction update.

Add integration coverage for concurrent writers and retries, proving that
the lifecycle snapshots cannot observe an inconsistent set of transaction
rows.

**Human review question:** is there one consistent lock-before-row-update
protocol for every single-batch aggregate writer?

### D6 - Align two-batch moves and post-approval effects

**Depends on:** D5.

Update:

- final-approval suspended reassignment;
- merchant postponement;
- invoice replacement membership movement;
- reversal detachment;
- any other operation that changes membership between two batches.

For operations involving two batches:

- create or find the target within the same SQL transaction;
- lock source and target rows in deterministic ID order;
- lock both rows before changing transaction membership;
- update the transaction membership exactly once;
- preserve initiative integrity;
- preserve `INVOICED`/`SUSPENDED` and last-elaborated-month semantics;
- leave all captured source and target snapshots unchanged.

Reversal detachment must update only live aggregates. Invoice replacement
must retain `CREATED` membership when required by the design and otherwise
move the transaction according to the existing outcome-month rule.

Add concurrency and retry tests for source/target races, duplicate target
creation, repeated events, equal or stale payment-impact revisions, and
opposite-direction moves.

**Human review question:** can two concurrent moves deadlock or leave a
transaction associated with two memberships?

### D7 - Harden compatibility and generic write paths

**Depends on:** D2, D3, D4, and D6.

Audit all remaining batch writes and ensure that:

- CSV filename updates do not overwrite lifecycle snapshots;
- delivery request amount remains the separate immutable Erogazioni
  snapshot;
- delivery selection uses the derived approved amount;
- refund and delivery outcome updates do not rewrite lifecycle snapshots;
- ordinary metadata updates do not persist effective live projection fields
  as raw snapshot values;
- list and detail endpoints expose identical effective values;
- no new endpoint, request parameter, status, or external contract is added.

Remove or narrow generic write methods where they can violate snapshot
immutability. Keep creation and non-snapshot metadata updates explicit.

Add regression tests for CSV generation, delivery amount capture, delivery
outcomes, refund outcomes, and post-approval reward/invoice/reversal changes.

**Human review question:** can any non-lifecycle write change either captured
snapshot after `SENT` or `APPROVING`?

### D8 - Complete integration and acceptance coverage

**Depends on:** D7.

Run the final PostgreSQL/Testcontainers and reactive verification matrix:

- state-matrix projections;
- signed amounts;
- current and excluded amounts;
- approved-amount state gate;
- empty batches;
- exact-once snapshot capture;
- retry-after-commit;
- missing-snapshot failures;
- concurrent assignment, decisions, reward updates, and two-batch moves;
- invoice replacement and reversal detachment;
- list/detail mapping and amount sorting;
- delivery and refund outcomes;
- initiative and single-membership constraints.

Update this plan and the design review references only if the implementation
reveals a required behavior change. Do not weaken tests to accommodate an
implementation shortcut.

## Agent briefing contract

Every coding-agent task should contain only one slice and use the following
structure:

```text
Implement slice D<N> only.

Authoritative design:
- docs/invitalia-reward-batch-counter-design-review.md
- Relevant sections: <section names>

Baseline:
- Previous slices merged: <list>
- Do not restart or refactor the completed SQL migration.

Task:
- <one concrete behavior or persistence outcome>

Required invariants:
- <slice-specific invariants>

Files to inspect first:
- <exact source files>
- <exact test files>

Tests:
- <focused unit tests>
- <focused PostgreSQL/Testcontainers tests>

Out of scope:
- <explicit exclusions>

Stop and report if:
- current behavior contradicts an assumption;
- the change requires a public contract change;
- the required transaction boundary cannot be preserved;
- unrelated refactoring appears necessary.

At completion, report:
- changed files;
- invariants covered;
- tests executed;
- unresolved risks or follow-up work.
```

The agent must not edit generated jOOQ sources by hand, introduce blocking
database calls, add `.block()` to application flows, add generic CRUD ports
solely for migration convenience, or change unrelated behavior.

## Human review gates

Review each slice against one primary question:

| Gate | Question |
| --- | --- |
| D0 | Is the repository gap analysis complete and based on the current branch? |
| D1 | Are raw snapshots nullable, distinct from zero, and protected from generic saves? |
| D2 | Are all effective aggregate formulas identical across reads and sorting? |
| D3 | Is `CREATED -> SENT` and its snapshot one atomic operation? |
| D4 | Is `EVALUATING -> APPROVING` and its snapshot one atomic operation? |
| D5 | Do all single-batch aggregate writers lock before changing transaction rows? |
| D6 | Are two-batch operations atomic and deterministically locked? |
| D7 | Are snapshots immutable through every remaining write path? |
| D8 | Does the full acceptance matrix pass without contract changes? |

## Quality requirements

For every production-code slice:

- identify affected branches, error paths, and invariants before editing;
- use `StepVerifier` for reactive unit tests;
- use focused PostgreSQL Testcontainers tests for schema, jOOQ, locking,
  aggregate, and transaction behavior;
- regenerate jOOQ sources through the repository build after schema changes;
- run the repository Java quality workflow, including coverage and Sonar
  checks, when the slice changes Java, SQL, Maven, or workflow files;
- do not merge or claim completion while focused tests, full tests, coverage,
  or high-confidence quality findings remain unresolved.

## Non-goals

This plan does not introduce:

- mutable application-maintained batch counters;
- a second transaction-membership table;
- a transaction snapshot or send-time Excel report;
- a new batch-routing or month-selection algorithm;
- a generic move endpoint;
- new public endpoints or status values;
- payment transaction cancellation or deletion from this service;
- production historical backfill or dual-write deployment logic.
