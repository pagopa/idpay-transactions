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
- Add optional `currentAmountCents` and `excludedAmountCents` fields to the
  existing `RewardBatchDTO`.
- Preserve the existing approved-amount formula and state vocabulary.
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
- Optional DTO fields and existing batch read responses.
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
| `approvedAmountCents` | Before evaluation, `0`; from `EVALUATING` onward, sum of `accrued_reward_cents` for `TO_CHECK`, `CONSULTABLE`, and `APPROVED` transactions |
| `currentAmountCents` | Sum of `accrued_reward_cents` for all transactions currently assigned to the batch, regardless of in-batch status |
| `excludedAmountCents` | Sum of `accrued_reward_cents` for currently assigned `REJECTED` transactions |

The existing formulas for approved amount and elaborated counts are not
changed by this design. In particular, `TO_CHECK` and `CONSULTABLE` remain
part of `approvedAmountCents` according to the current SQL projection.

`TO_APPROVE` and `TO_WORK` remain query translations based on batch status and
assignee level. They are not transaction statuses and do not participate
directly in amount aggregation.

## Snapshot lifecycle

### Initial amount snapshot

While a batch is `CREATED`, the initial amount is the current live aggregate
over all assigned transactions.

During the atomic `CREATED -> SENT` transition:

1. Lock or conditionally claim the batch according to the existing send rules.
2. Calculate the live sum of `accrued_reward_cents` over its assigned
   transactions.
3. Persist that value as the initial amount snapshot.
4. Persist the send timestamp and transition the batch to `SENT`.

The snapshot is immutable after the transition. Later reward updates,
detachment, reassignment, or other membership changes do not change
`initialAmountCents`.

An empty batch is valid. Its initial snapshot is the numeric value `0`, which
must be distinguishable from an unset snapshot during persistence and reads.

### Suspended amount snapshot

Before the snapshot is created, `suspendedAmountCents` is the live sum of
`accrued_reward_cents` for assigned `SUSPENDED` transactions.

During the atomic `EVALUATING -> APPROVING` transition:

1. Lock or conditionally claim the batch according to the existing approval
   rules.
2. Calculate the live suspended amount.
3. Persist that value as the suspended amount snapshot.
4. Persist the approval-transition timestamp and set the batch to `APPROVING`.

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

Add two nullable, immutable-after-capture columns to `reward_batches`, using
physical names consistent with the migration conventions, for example:

```text
initial_amount_cents_at_send BIGINT NULL
suspended_amount_cents_at_approving BIGINT NULL
```

The columns are nullable only before their lifecycle transition. A stored zero
is a valid captured value and must not be treated as missing.

The batch read projection should expose:

- the stored initial snapshot when it exists, otherwise the live initial
  aggregate while the batch is still `CREATED`;
- the stored suspended snapshot when it exists, otherwise the live suspended
  aggregate;
- the live current and excluded aggregates;
- all existing live count projections.

The existing covering index on batch membership, in-batch status, and
`accrued_reward_cents` remains the basis for the live projections.

The existing `delivery_amount_cents` field remains a separate immutable
snapshot of the amount sent to Erogazioni. It must not be reused for
`initialAmountCents`, `approvedAmountCents`, or `currentAmountCents`.

## Atomicity and concurrency

Snapshot capture and the corresponding lifecycle transition must be one SQL
transaction. A successful transition must never commit without its snapshot,
and a snapshot must never be committed without the corresponding transition.

Operations that can change the assigned transaction set or its in-batch state
must use the same batch-locking or conditional-update strategy already used by
the SQL design. This includes:

- invoiced transaction assignment;
- operator decisions;
- final-approval suspended reassignment;
- merchant postponement;
- payment-driven invoice replacement;
- payment-driven reversal detachment.

The send path must serialize against assignment. The approval path must
serialize against transaction decisions and membership moves. Retrying either
transition must not overwrite an existing snapshot.

The transaction row continues to have at most one current `reward_batch_id`.
Moving a transaction updates that assignment; it does not create a second
membership record.

## Post-approval behavior

The existing payment ownership and event-driven local projection boundaries
remain unchanged.

### Reversal of an excluded transaction

For a permitted reversal:

- the existing `TRANSACTION_REFUNDED` handling persists the local transaction
  projection and detaches the current batch membership;
- the source batch's live current amount and excluded amount decrease;
- the source batch's initial and suspended snapshots remain unchanged;
- no payment cancellation or deletion call is introduced.

### Invoice replacement of an excluded transaction

For a permitted invoice replacement:

- the existing payment impact contract remains authoritative;
- the transaction is removed from the source batch membership;
- the existing current-month find-or-create routine is used for the target
  batch;
- the transaction is associated with that target batch as `SUSPENDED`;
- source and target live aggregates are recalculated from their current rows;
- any already captured source snapshot remains unchanged.

This design does not add a generic move endpoint and does not invent a new
month-selection algorithm.

## API and DTO changes

The existing batch endpoints and response shape remain in use. Extend
`RewardBatchDTO` with nullable `Long` fields:

```java
Long currentAmountCents;
Long excludedAmountCents;
```

The fields are optional for compatibility with clients that do not consume
them. The existing `@JsonInclude(JsonInclude.Include.NON_NULL)` behavior may
omit them when a projection does not provide a value.

The internal `RewardBatch` projection and mapper must carry the same values so
that list and detail responses use the same database-side definitions.

Existing fields retain their names:

- `initialAmountCents` becomes the live value before send and the immutable
  send snapshot afterward;
- `suspendedAmountCents` becomes the live value before `APPROVING` and the
  immutable approval-transition snapshot afterward;
- `approvedAmountCents` keeps the current state-aware formula.

No new endpoint, request parameter, authorization rule, or external contract
is required.

## Migration and compatibility

The migration adds only the two snapshot columns and any required generated
SQL metadata. It does not restore the mutable counter columns removed by the
aggregate-projection design.

Because the system is not in production, no historical reconstruction of
send-time or approval-time values is required. New test fixtures and local
data must exercise the lifecycle transitions that populate the snapshots.

The migration must preserve string identifiers, the composite initiative/batch
integrity rule, and all existing live aggregate indexes.

## Required tests

Add focused reactive unit and PostgreSQL integration coverage for:

1. A `CREATED` batch reports live initial and current amounts.
2. Assignment or reward changes before send change the live initial amount.
3. `CREATED -> SENT` captures the initial amount exactly once.
4. A zero-transaction batch can be sent and captures an initial value of `0`.
5. Changes after send do not change the initial snapshot.
6. Suspended amount remains live before `APPROVING`.
7. `EVALUATING -> APPROVING` captures the suspended amount exactly once.
8. A batch with no suspended transactions captures a suspended value of `0`.
9. Later suspended reassignment does not change the suspended snapshot.
10. Current and excluded amounts follow the assigned rows and the five
    persisted in-batch statuses.
11. `approvedAmountCents` preserves the existing
    `TO_CHECK + CONSULTABLE + APPROVED` formula.
12. Reversal detachment changes live aggregates but not captured snapshots.
13. Invoice replacement moves the transaction through the existing current-
    month routine and updates source and target live aggregates atomically.
14. Concurrent assignment/send and decision/approval attempts cannot capture
    inconsistent snapshots.
15. Retrying a lifecycle transition does not overwrite a captured snapshot.
16. Empty batches can complete the ordinary send and approval lifecycle.
17. The new DTO fields are optional and are populated consistently for list and
    detail responses.

## Acceptance criteria

- The existing SQL aggregate formulas remain unchanged unless explicitly
  overridden by the two snapshot rules in this document.
- `initialAmountCents` is captured exactly on entry into `SENT` and is
  immutable afterward.
- `suspendedAmountCents` is captured exactly on entry into `APPROVING` and is
  immutable afterward.
- All other counters and `currentAmountCents`/`excludedAmountCents` remain
  live projections over currently assigned transaction rows.
- In-batch transaction status is the only status classification used by the
  counter queries.
- Empty batches remain sendable and approvable.
- Post-approval reversal and invoice replacement preserve current payment
  ownership and existing batch-impact routines.
- No second membership table, mutable counter-update workflow, new routing
  algorithm, or transaction snapshot is introduced.
- Existing batch endpoints remain compatible, with only the two optional DTO
  fields added.

## References

- `docs/Gestione_Lotto_Invitalia.md`
- `docs/DR-rework-reward-batches-in-sql.md`
- `docs/implementation-plan-rework-reward-batches-in-sql.md`
- `src/main/java/it/gov/pagopa/idpay/transactions/persistence/sql/SqlRewardBatchListAdapter.java`
- `src/main/java/it/gov/pagopa/idpay/transactions/dto/RewardBatchDTO.java`
- `src/main/resources/db/migration/V004__derive_reward_batch_aggregates.sql`
- `src/main/java/it/gov/pagopa/idpay/transactions/persistence/sql/SqlRewardBatchDeliveryAdapter.java`
- `docs/idpay-payment-reward-batch-impact.md`
