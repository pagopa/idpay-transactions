# idpay-payment impact on reward batches

## Ownership and event contract

`idpay-payment` owns payment state, invoice commands, invoice blobs, and the
canonical transaction snapshot. `idpay-transactions` owns reward-batch
membership and in-batch state.

Payment publishes one immutable event for each
`(transactionId, transactionRevision)` on the existing `idpay-transaction`
topic, keyed by transaction ID:

| Outcome | Event type | Payload status |
| --- | --- | --- |
| Initial invoice | `TRANSACTION_INVOICED` | `INVOICED` |
| Invoice replacement | `TRANSACTION_INVOICE_REPLACED` | `INVOICED` |
| Reversal | `TRANSACTION_REFUNDED` | `REFUNDED` |

An invoice replacement does not also publish `TRANSACTION_INVOICED` or a
dedicated `INVOICE_REPLACED` impact. `INVOICED_REVERSED` is not part of this
contract.

The flat payload contains all canonical `RewardTransactionDTO` fields plus:

```text
eventId
schemaVersion
eventType
occurredAt
transactionRevision
```

`eventType` classifies the committed event. `operationType` retains its
payment-domain meaning and must not be overwritten with an event type.
`transactionRevision` is independent from reward-calculator `counterVersion`.

The payload contains payment-owned canonical data only. It must not contain
reward-batch ID, in-batch status, rejection reason, inclusion date, last
elaborated month, sampling key, or checks error.

## Replacement handling

`rewardTrxConsumer` classifies `TRANSACTION_INVOICE_REPLACED` from payload
`eventType`. It validates that:

- `eventId` is present;
- `schemaVersion` is positive;
- `occurredAt` is present;
- `transactionRevision` is positive and matches the canonical snapshot;
- status is `INVOICED`;
- exactly one initiative is present;
- merchant ID, point-of-sale type, and business name are present; and
- no local reward-batch fields are supplied by payment.

The consumer applies the projection and reward-batch effect in one local SQL
transaction. The stored `transaction_revision` is the only ordering and
idempotency boundary: equal or stale revisions are no-ops.

Membership is read and locked at handling time:

| Membership state | Effect |
| --- | --- |
| No membership | Update the canonical projection only |
| Source batch `CREATED` | Keep membership and in-batch state unchanged |
| Any other supported source state | Move to the outcome-month grouping and set membership to `SUSPENDED` |

The target grouping uses initiative, merchant, point-of-sale type, and
`occurredAt` converted to `Europe/Rome`. Batch counters remain derived from
committed membership rows.

Kafka acknowledgement occurs only after the reactive SQL transaction
completes. Validation or persistence errors use the existing transaction
consumer error notification and retry path.

## Compatibility

Messages without `eventType` continue through the existing status-based
handling. `TRANSACTION_INVOICED` and `TRANSACTION_REFUNDED` behavior is
unchanged. No second consumer binding, topic, or impact watermark is required.

Production of `TRANSACTION_INVOICE_REPLACED` remains gated until every
blocking consumer identified by the payment contract is compatible.

## Implementation status

| Item | Status |
| --- | --- |
| Transaction revision storage and revision-ordered projection | Implemented |
| `TRANSACTION_REFUNDED` persist-and-detach | Implemented |
| Read-only invoice lifecycle eligibility endpoint | Implemented |
| Unified replacement classification on `idpay-transaction` | Implemented by PR 02 |
| Atomic replacement projection and membership transition | Implemented by PR 02 |
| Payment immutable replacement producer | Pending later payment PRs |
