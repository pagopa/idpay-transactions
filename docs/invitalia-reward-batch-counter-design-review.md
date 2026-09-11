# Design Review: Invitalia Reward-Batch Counter Lifecycle

## Status

Agreed design for implementation.

## Context

`docs/Gestione_Lotto_Invitalia.md` defines the lifecycle of the reward-batch
amounts shown during merchant and Invitalia processing.

This is an evolution of the existing reactive SQL design. The current design
correctly derives batch counters from the assigned transaction rows and their
in-batch status. The new requirement adds two historical values that must be
captured at lifecycle transitions and must not be recalculated afterward:

1. `initialAmountCents` when the batch enters `SENT`;
2. `suspendedAmountCents` when the batch enters `APPROVING`.

The design does not restore mutable application-maintained counters. Live
projections remain database-side aggregates; only the two explicitly required
snapshots are persisted.

For `initialAmountCents` and `suspendedAmountCents`, this document supersedes
the earlier live-only definitions in `docs/DR-rework-reward-batches-in-sql.md`.
All other aggregate and lifecycle rules from that document remain unchanged.

## Decision summary

- Keep PostgreSQL, R2DBC, jOOQ, reactive transactions, and the existing
  single-row transaction membership model.
- Continue using `accrued_reward_cents` as the monetary source.
- Continue using `reward_batch_trx_status` for all in-batch counter
  classification.
- Persist two immutable batch snapshots:
  - the initial amount captured on `CREATED -> SENT`;
  - the suspended amount captured on `EVALUATING -> APPROVING`.
- Keep all other counters and amounts live.
- Add additive `currentAmountCents` and `excludedAmountCents` fields to the
  existing `RewardBatchDTO`.
- Preserve the existing approved-amount formula and state vocabulary.
- Capture each snapshot with a batch-row lock and one transaction-bound SQL
  operation. Lifecycle retries are idempotent and never overwrite a snapshot.
- Use the existing routine that finds or creates a batch in the current month
  for post-approval invoice replacement. This design does not introduce a new
  definition of "next batch".
- Empty batches remain eligible for send and approval. No special empty-batch
  lifecycle is introduced.
- Transaction snapshots and a send-time Excel report are out of scope.

## Scope

### In scope

- Persistent storage for the two lifecycle snapshots.
- Live aggregate projections for current and excluded amounts.
- Atomic snapshot capture at the two lifecycle transitions.
- Additive DTO fields and existing batch read responses.
- Interaction with assignment, decision, approval, reversal, and invoice
  replacement flows.
- Reactive and PostgreSQL integration tests for the new invariants.

### Out of scope

- A transaction snapshot or Excel report at send time.
- New public endpoints.
- New batch-routing or month-selection rules.
- Changes to payment ownership or external invoice lifecycle contracts.
- Changes to the existing reward-batch or in-batch status vocabulary.
- Historical production backfill. The system is not in production, so existing
  non-production data does not require reconstruction of historical snapshots.
- Replacement of the existing Erogazioni delivery snapshot.

## Authoritative counter definitions

All monetary aggregates use the typed `accrued_reward_cents` value stored on
`reward_transactions`. No conversion to transaction purchase amount or other
reward fields is introduced.

All state-specific calculations use `reward_batch_trx_status`, not the
canonical payment transaction status. The persisted in-batch statuses remain:

```text
TO_CHECK
CONSULTABLE
SUSPENDED
APPROVED
REJECTED
```

The following existing formulas remain authoritative:

| Counter | Definition |
| --- | --- |
| `numberOfTransactions` | Count of all transactions currently assigned to the batch |
| `numberOfTransactionsElaborated` | Count of `SUSPENDED`, `APPROVED`, and `REJECTED` transactions |
| `numberOfTransactionsSuspended` | Count of `SUSPENDED` transactions |
| `numberOfTransactionsRejected` | Count of `REJECTED` transactions |
| `approvedAmountCents` | `0` for `CREATED` and `SENT`; from `EVALUATING` onward, sum of `accrued_reward_cents` for `TO_CHECK`, `CONSULTABLE`, and `APPROVED` transactions |
| `currentAmountCents` | Sum of `accrued_reward_cents` for all transactions currently assigned to the batch, regardless of in-batch status |
| `excludedAmountCents` | Sum of `accrued_reward_cents` for currently assigned `REJECTED` transactions |

The approved-amount state gate is part of the formula, not only a presentation
rule. The same expression must be used by batch projections, sorting, and any
query that consumes the approved amount. In particular, `TO_CHECK` and
`CONSULTABLE` remain part of `approvedAmountCents` from `EVALUATING` onward.

`TO_APPROVE` and `TO_WORK` remain query translations based on batch status and
assignee level. They are not transaction statuses and do not participate
directly in amount aggregation.

`accrued_reward_cents` is a signed typed value. Aggregates preserve negative
values carried by refund or cancellation events; no non-negative assumption is
introduced by this design.

## Snapshot lifecycle

Both transitions use the same mechanics:

1. Open one reactive SQL transaction and lock the batch row with `SELECT FOR
   UPDATE` using the transaction-bound SQL connection.
2. Validate the existing lifecycle rules while the lock is held.
3. Calculate the required live aggregate.
4. In the same transaction, persist the snapshot, lifecycle timestamp, and
   target status.

### Initial amount snapshot

While a batch is `CREATED`, the initial amount is the current live aggregate
over all assigned transactions.

For `CREATED -> SENT`, the required aggregate is the sum of
`accrued_reward_cents` over all assigned transactions.

The snapshot is immutable after the transition. Later reward updates,
detachment, reassignment, or other membership changes do not change
`initialAmountCents`.

An empty batch is valid. Its initial snapshot is the numeric value `0`, which
must be distinguishable from an unset snapshot during persistence and reads.

### Suspended amount snapshot

Before the snapshot is created, `suspendedAmountCents` is the live sum of
`accrued_reward_cents` for assigned `SUSPENDED` transactions.

For `EVALUATING -> APPROVING`, the required aggregate is the sum of
`accrued_reward_cents` for assigned `SUSPENDED` transactions.

The snapshot is immutable after the transition. It remains the value exposed
as `suspendedAmountCents`, even if the final-approval worker subsequently moves
suspended transactions to another batch.

An empty or fully non-suspended batch receives a suspended snapshot of `0`.

The exact freeze point is therefore entry into `APPROVING`, not completion of
the final approval worker and not entry into `APPROVED`.

## SQL data model

The SQL design must continue to derive mutable operational counters from
`reward_transactions`. It must not reintroduce columns that are incremented or
decremented by every transaction decision.

Add two nullable, immutable-after-capture columns to `reward_batches`:

```text
initial_amount_cents_at_send BIGINT NULL
suspended_amount_cents_at_approving BIGINT NULL
```

The columns are nullable only before their lifecycle transition. A stored zero
is a valid captured value and must not be treated as missing.

The batch read projection must use this state matrix:

| Batch status | `initialAmountCents` | `suspendedAmountCents` |
| --- | --- | --- |
| `CREATED` | Live aggregate | Live aggregate |
| `SENT`, `EVALUATING` | Stored send snapshot | Live aggregate |
| `APPROVING`, `APPROVED`, `PENDING_REFUND`, `NOT_REFUNDED`, `REFUNDED` | Stored send snapshot | Stored approval snapshot |

Do not use `COALESCE(snapshot, live)` after the freeze state. A missing
snapshot after `SENT` or `APPROVING` must fail the read with an explicit
data-integrity error. Reconciliation may report the same condition
separately, but it must not silently substitute a new live value.

The live current, excluded, and count projections remain database-side
aggregates. The effective amount expressions above must also be used for
amount sorting. Keep the raw snapshot columns separate from the effective
`RewardBatch` projection fields so a generic save cannot write a live
projection back into an immutable snapshot.

The existing covering index on batch membership, in-batch status, and
`accrued_reward_cents` remains the basis for the live projections.

The existing `delivery_amount_cents` field remains a separate immutable
snapshot of the amount sent to Erogazioni. It must not be reused for
`initialAmountCents`, `approvedAmountCents`, or `currentAmountCents`.

## Atomicity and concurrency

Snapshot capture and the corresponding lifecycle transition must use one
reactive SQL transaction and one transaction-bound SQL connection. A
successful transition must never commit without its snapshot, and a snapshot
must never be committed without the corresponding transition. Do not implement
these transitions as a read, in-memory mutation, and generic full-row save.

Every operation that can change a batch aggregate must acquire the same batch
row lock before changing the relevant transaction rows. This includes:

- invoiced transaction assignment;
- operator decisions;
- final-approval suspended reassignment;
- merchant postponement;
- payment-driven invoice replacement;
- payment-driven reversal detachment;
- reward synchronization that changes `accrued_reward_cents`, membership, or
  in-batch status.

The send path must serialize against assignment and reward updates. The
approval path must serialize against transaction decisions and membership
moves. For a move affecting two batches, lock both batch rows in deterministic
ID order.

Lifecycle mutations must be guarded by the expected old status and a null
snapshot. A retry after a committed transition returns the existing target
state and snapshot successfully; it never overwrites the snapshot. An
unexpected state or a missing snapshot after the freeze point must fail with
an explicit conflict or data-integrity error.

The transaction row continues to have at most one current `reward_batch_id`.
Moving a transaction updates that assignment; it does not create a second
membership record.

### Payment-impact idempotency

Payment-driven invoice replacement and reversal handling use
`transactionRevision` as the sole ordering and idempotency boundary. Equal or
stale revisions are no-ops. This design does not introduce an impact inbox or
a second payment-impact watermark.

The existing `latest_applied_payment_impact_revision` schema field, if
retained for compatibility, is not authoritative for this contract and must
not establish a second idempotency boundary.

## Post-approval behavior

The existing payment ownership and event-driven local projection boundaries
remain unchanged. The payment impact contract and current-month
find-or-create routine remain authoritative.

Post-approval reversal detaches the local membership and changes only the
source batch's live aggregates. Invoice replacement detaches the source
membership and associates the transaction with the existing target batch as
`SUSPENDED`; source and target live aggregates are recalculated from their
current rows. In both cases, captured snapshots never change and no payment
cancellation or deletion call is introduced.

No generic move endpoint or new month-selection algorithm is introduced.

## API and DTO changes

The existing batch endpoints and response shape remain in use. Extend
`RewardBatchDTO` with `Long` fields:

```java
Long currentAmountCents;
Long excludedAmountCents;
```

The fields are additive for clients and must be populated with `0` when the
aggregate is empty. They should not be omitted merely because one read path
failed to populate a defined aggregate.

The internal `RewardBatch` projection and mapper must carry the same values so
that list and detail responses use the same database-side definitions. Raw
snapshot columns remain persistence fields; `initialAmountCents` and
`suspendedAmountCents` are the effective values defined by the state matrix.

Existing fields retain their names:

- `initialAmountCents` becomes the live value before send and the immutable
  send snapshot afterward;
- `suspendedAmountCents` becomes the live value before `APPROVING` and the
  immutable approval-transition snapshot afterward;
- `approvedAmountCents` keeps the current state-aware formula.

No new endpoint, request parameter, authorization rule, or external contract
is required.

## Migration and compatibility

The next ordered migration adds the two nullable columns defined above. It
also regenerates the jOOQ metadata and updates the persistence mapper. It does
not restore the mutable counter columns removed by the aggregate-projection
design.

Because the system is not in production, no historical reconstruction of
send-time or approval-time values is required. New test fixtures and local
data must exercise the lifecycle transitions that populate the snapshots.

The migration must preserve string identifiers, the composite initiative/batch
integrity rule, and all existing live aggregate indexes.

## Required tests

Add focused reactive unit and PostgreSQL integration coverage for:

1. The state matrix, including live versus frozen values, zero amounts,
   current/excluded aggregates, signed amounts, and the approved-amount gate.
2. Both lifecycle transitions, including atomicity, empty batches, exact-once
   capture, retry-after-commit, and missing-snapshot failures.
3. Concurrent assignment, decisions, reward updates, and two-batch moves,
   proving that snapshots cannot capture inconsistent rows.
4. Reversal detachment and invoice replacement, proving that live aggregates
   change while captured snapshots remain unchanged.
5. List/detail mapping and amount sorting, proving the same effective values
   are returned and ordered.
6. Payment-impact retries with equal and stale `transactionRevision` values,
   proving that they are no-ops without a second inbox or watermark.

## Acceptance criteria

- `initialAmountCents` is live only in `CREATED`, then is captured exactly on
  entry into `SENT` and never overwritten.
- `suspendedAmountCents` is live through `EVALUATING`, then is captured exactly
  on entry into `APPROVING` and never overwritten.
- `approvedAmountCents` is `0` in `CREATED` and `SENT`, and otherwise sums
  `TO_CHECK`, `CONSULTABLE`, and `APPROVED` transaction rows.
- All other counters and `currentAmountCents`/`excludedAmountCents` remain
  live projections over currently assigned transaction rows.
- Counter classification uses only the five persisted in-batch statuses;
  `TO_APPROVE` and `TO_WORK` remain query-only translations.
- Every aggregate-affecting writer uses the shared batch-locking strategy, and
  lifecycle retries are idempotent.
- Empty batches remain sendable and approvable under the ordinary lifecycle
  rules; post-approval impacts preserve payment ownership and do not change
  captured snapshots.
- Payment-driven impacts use `transactionRevision` as their sole ordering and
  idempotency boundary; equal and stale revisions are no-ops.
- No second membership table, mutable counter-update workflow, new routing
  algorithm, or transaction snapshot is introduced. Existing endpoints remain
  compatible with the additive DTO fields.

## References

- `docs/Gestione_Lotto_Invitalia.md`
- `docs/DR-rework-reward-batches-in-sql.md`
- `docs/implementation-plan-rework-reward-batches-in-sql.md`
- `src/main/java/it/gov/pagopa/idpay/transactions/persistence/sql/SqlRewardBatchListAdapter.java`
- `src/main/java/it/gov/pagopa/idpay/transactions/dto/RewardBatchDTO.java`
- `src/main/resources/db/migration/V004__derive_reward_batch_aggregates.sql`
- `src/main/java/it/gov/pagopa/idpay/transactions/persistence/sql/SqlRewardBatchDeliveryAdapter.java`
- `docs/idpay-payment-reward-batch-impact.md`
