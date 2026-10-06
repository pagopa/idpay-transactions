# Implementation Plan: TRANSACTION_REFUNDED batch detach

## Status

Proposed.

## Scope

Change how an invoiced reversal updates local reward-batch membership.

Payment remains the owner of transaction state. When a transaction is
reversed, payment writes `TRANSACTION_REFUNDED` to its transaction outbox.
CDC publishes that snapshot to the existing `idpay-transaction` topic.
`rewardTrxConsumer` must persist status `REFUNDED` and detach the current
reward-batch association.

This plan does not rewrite
[implementation-plan-rework-reward-batches-in-sql.md](implementation-plan-rework-reward-batches-in-sql.md).
That document is the completed SQL rework and stays immutable. The decision
record may describe this newer contract; this plan is the work that implements
it.

## Non-goals

- Payment CDC, outbox schema, or publication of `TRANSACTION_REFUNDED`.
- Invoice replacement. `INVOICE_REPLACED` and its dedicated impact stay as
  implemented.
- Public REST contracts, batch lifecycle states, or derived batch aggregates.
- Re-opening Mongo cutover, schema migrations, or dual-write work.

## Current state

- `PersistenceTransactionMediatorImpl` skips messages whose
  `operationType` header is `REFUNDED` and does not persist them.
- `PaymentRewardBatchImpactType` includes `INVOICED_REVERSED`.
- `SqlPaymentRewardBatchImpactAdapter` detaches membership only when that
  dedicated impact is applied.
- The runtime dedicated-impact consumer binding is not wired.
- [idpay-payment-reward-batch-impact.md](idpay-payment-reward-batch-impact.md)
  documents reversal as `TRANSACTION_REFUNDED` on `idpay-transaction`.
  `INVOICE_REPLACED` is the only dedicated impact.

## Target behaviour

| Signal | Publication | Local effect |
| --- | --- | --- |
| Invoice replacement | Dedicated `INVOICE_REPLACED` impact | Unchanged |
| Reversal | Outbox `TRANSACTION_REFUNDED` → CDC → `idpay-transaction` | Persist `REFUNDED`. If `reward_batch_id` is set, clear membership and in-batch assignment fields. If it is already null, persist `REFUNDED` and leave membership unchanged. |

`rewardTrxConsumer` must persist and detach from payload `status=REFUNDED`.
The payment CDC connector publishes the outbox `payload` only and does not
set an `operationType` header; a header must not rewrite status. The generic
snapshot still must not overwrite local batch fields except to clear them on
detach.

The generic topic is only the transport for this signal. At the application
boundary, transaction synchronization must route `REFUNDED` snapshots to a
focused persist-and-detach operation. The ordinary SQL projection upsert must
remain status-agnostic and must not contain a hidden `REFUNDED` branch.

The persist-and-detach operation is a reward-batch impact in domain terms, but
it must not be forced through `PaymentRewardBatchImpact` or
`SqlPaymentRewardBatchImpactAdapter`. That dedicated envelope, impact
watermark, and equal-revision handling remain specific to
`INVOICE_REPLACED`. Reversal continues to use the generic snapshot's strict
transaction-revision ordering.

Detach is idempotent: a retry of an already detached `REFUNDED` row must not
fail or recreate membership. Revision rules stay as implemented: a stale
snapshot must not overwrite a newer local projection.

`INVOICED_REVERSED` is removed from this service. The impact enum, adapter
branch, validation, and tests keep only `INVOICE_REPLACED`.

## Thin PR sequence

| PR | Deliverable | Depends on |
| --- | --- | --- |
| 01 | Update [idpay-payment-reward-batch-impact.md](idpay-payment-reward-batch-impact.md) so reversal is `TRANSACTION_REFUNDED` on `idpay-transaction`. Keep `INVOICE_REPLACED` as the only dedicated impact. Point the DR at this plan. Do not change Java. | — |
| 02 | Stop skipping `REFUNDED` in `rewardTrxConsumer`. Route the mapped snapshot at the application layer to a focused persist-and-detach port operation; keep the ordinary projection upsert status-agnostic. Persist the snapshot and detach current membership in one SQL transaction. Cover assigned detach, already-unassigned persist, retry/idempotency, and stale-revision ignore. | 01 |
| 03 | Remove `INVOICED_REVERSED` from `PaymentRewardBatchImpactType`, `SqlPaymentRewardBatchImpactAdapter`, and its tests. Keep `INVOICE_REPLACED` behaviour unchanged. | 02 |

## Implementation notes

- Reuse the existing membership-clear columns used by the current detach
  update: `reward_batch_id`, `reward_batch_trx_status`,
  `reward_batch_inclusion_date`, and `sampling_key`.
- Dispatch by payload status before entering the ordinary synchronization
  adapter: `INVOICED` keeps its assignment flow, `REFUNDED` uses the focused
  persist-and-detach operation, and other statuses use the projection upsert.
- Keep `SqlRewardTransactionAdapter.upsert` status-agnostic. Shared SQL
  construction and revision guards may be reused, but the reversal decision
  belongs to the application boundary.
- Do not synthesize a `PaymentRewardBatchImpact` for reversal and do not use
  the dedicated impact watermark. `TRANSACTION_REFUNDED` remains ordered and
  deduplicated by the generic transaction revision.
- Keep batch amounts/counts derived from remaining assigned rows. Do not
  write batch counters.
- Do not call payment, cancel a payment transaction, or touch invoice blobs.
- Payment enabling CDC for `TRANSACTION_REFUNDED` is a cross-repo
  prerequisite for the live path. This service must still accept and apply
  the snapshot once published.

## Tests

- Consumer persists payload `status=REFUNDED` and does not rewrite status from an `operationType` header.
- Assigned transaction becomes `REFUNDED` and loses batch membership.
- Unassigned transaction becomes `REFUNDED` and stays unassigned.
- Retry of the same `REFUNDED` snapshot is a no-op after the first apply.
- Stale `REFUNDED` snapshot does not overwrite a newer revision.
- `INVOICE_REPLACED` still keeps a `CREATED` membership and still moves a
  non-`CREATED` membership to the outcome-month grouping as `SUSPENDED`.
- No remaining production or test reference to `INVOICED_REVERSED`.

## Quality gate

Follow `.github/instructions/java-quality.instructions.md` for PRs 02 and 03.
Use `StepVerifier` for the consumer and Testcontainers PostgreSQL for the
detach write. Docker is required for those integration tests.
