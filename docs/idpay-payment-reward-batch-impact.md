# idpay-payment impact: reward-batch invoice lifecycle

## Purpose and maintenance

This is the living cross-service handoff for payment-driven invoice replacement
and invoiced reversal. Update it in the same PR that changes either service's
transaction revision, generic transaction snapshot, eligibility query, impact
event, ordering, idempotency, or rollout behavior.

The payment service remains the owner of invoice/reversal commands,
authorization, blob operations, and authoritative transaction state.
`idpay-transactions` remains the owner of reward-batch membership and
in-batch evaluation state. Payment must not write reward-batch data directly.
An `INVOICED` generic snapshot updates only the local projection:
`idpay-transactions` must not cancel or delete payment's transaction.

## Current integration status

| Item | Status |
| --- | --- |
| Local SQL projection, revision storage, and transaction-local impact watermark | Implemented on the `idpay-transactions` PR 20 branch |
| Revision in `idpay-payment` transaction storage/model | Not implemented |
| Revision in generic transaction snapshots | Not implemented |
| Dedicated payment-to-reward-batch-impact producer | Not implemented |
| Read-only eligibility endpoint/client contract | Endpoint implemented in `idpay-transactions`; payment client pending |
| Runtime impact consumer binding in `idpay-transactions` | Not implemented |

The current payment `TransactionInProgress` model has `counterVersion`, but it
is the reward-calculator counter ETag. It is not a transaction lifecycle
revision and must not be reused for this integration.

The direct SQL cutover removes the legacy local invoice replacement and
invoiced-reversal routes from `idpay-transactions`. Until the payment contract
and runtime consumer binding are implemented, deployment must not rely on a
local substitute for those payment-owned commands.

## Required payment changes

### 1. Persist a transaction lifecycle revision

Add a distinct, non-negative `transactionRevision` to payment's authoritative
transaction record. It must:

- be initialized for every transaction;
- be incremented atomically whenever payment changes canonical transaction
  data that it publishes;
- be persisted in the same transaction as the invoice replacement or reversal
  outcome;
- be included in every generic transaction snapshot; and
- use the same value in the dedicated impact event and its embedded
  transaction projection.

The revision is transaction-scoped and monotonically increasing. It is not an
event sequence, a Kafka offset, an update timestamp, or `counterVersion`.

`idpay-transactions` stores the revision on `reward_transactions`. Generic
snapshots update the local canonical projection only when their revision is
strictly newer. It separately stores the latest payment-impact revision whose
handling committed. Generic snapshots never change that impact watermark, so
an impact at the same revision as a generic snapshot still applies once.

### 2. Extend the generic transaction snapshot

Add `transactionRevision` to the payment-produced `RewardTransactionDTO`.
This is additive at the wire level, but it is required before SQL cutover:
missing values map to revision `0` in the current receiver and cannot advance
an already synchronized local projection.

The generic snapshot must continue to contain only payment-owned transaction
data. It must not include or overwrite reward-batch membership/evaluation
fields.

### 3. Publish a dedicated impact event

For each successful payment operation below, persist an outbox record in the
same transaction as the authoritative payment update and publish it after
commit. Retries must retain the exact same event identity and payload.

| Payment operation | `impactType` | Required projected status |
| --- | --- | --- |
| Invoice replacement | `INVOICE_REPLACED` | `INVOICED` |
| Reversal of an invoiced transaction | `INVOICED_REVERSED` | `REFUNDED` |

Use the transaction ID as the message key. The event envelope is:

```text
eventId: String, unique and stable across delivery retries
schemaVersion: integer, initially 1
impactType: INVOICE_REPLACED | INVOICED_REVERSED
occurredAt: OffsetDateTime of the committed payment outcome
transactionRevision: positive long
transaction: canonical post-operation RewardTransactionDTO
```

The embedded transaction must have the same positive `transactionRevision` as
the envelope and must contain:

- its ID;
- exactly one initiative;
- the authoritative merchant ID;
- the post-operation status;
- all payment-owned canonical fields that a generic snapshot carries; and
- `pointOfSaleType` for `INVOICE_REPLACED`.

The event must not carry local reward-batch fields. In particular,
`rewardBatchId`, in-batch status, in-batch rejection reason, inclusion date,
last elaborated month, sampling key, and checks error must be absent/default.
The receiver rejects impacts that contain them.

`eventId` is retained for outbox correlation and tracing. Consumer
idempotency is based on the transaction revision: a transaction revision can
identify only one impact, and payment must never emit two different impacts at
the same revision.

### 4. Call the read-only eligibility query before the payment commit

Before changing invoice/reversal state, payment must obtain the current
reward-batch facts through the narrow read-only operation:

```text
findEligibility(merchantId, transactionId)
```

When a local membership exists, it returns:

```text
transactionId
initiativeId
merchantId
rewardBatchId
transactionStatus
batchStatus
batchTransactionStatus
```

#### Proposed HTTP contract

`idpay-transactions` exposes the read-only query as:

```http
GET /idpay/transactions/{transactionId}/reward-batch/eligibility?merchantId={merchantId}
```

The response is `200 OK` with the fields above when the transaction has a
current batch membership. It is `204 No Content` when no membership matches
the transaction and merchant, allowing payment to continue without a local
batch precondition. Missing request parameters are rejected with `400 Bad
Request`; database or service failures are not converted into an empty
eligibility response.

The client implementation and payment policy using this result are still an
integration PR decision. The query does not authorize the payment command,
does not reserve membership, and does not create write coupling. Its result
must not be copied into the impact event as a membership precondition because
the membership may change before event delivery.

## Effects applied by idpay-transactions

The impact handler reads the membership that exists when it processes the
event, validates the canonical payment projection, and applies one local SQL
transaction.

| Impact | Local reward-batch effect |
| --- | --- |
| `INVOICE_REPLACED` from a `CREATED` source batch | Keep the existing membership and in-batch state. |
| `INVOICE_REPLACED` from any other source batch state | Move the current membership to the grouping `(initiative, merchant, POS type, outcome month)`, create the target batch only if absent, and set its in-batch state to `SUSPENDED`. |
| `INVOICED_REVERSED` | Detach the current membership and clear its local in-batch assignment data. |

The outcome month is derived from `occurredAt` in `Europe/Rome`, making
retries deterministic across a month boundary. Batch counts and amounts are
derived from the committed transaction rows, so payment does not update batch
counters.

## Ordering, retries, and rollout

- Delivery is at least once. Payment must use a durable outbox or equivalent
  post-commit publication mechanism.
- Generic snapshots and the dedicated impact may arrive in either order. They
  must carry the same revision for the same payment outcome.
- A stale generic snapshot cannot overwrite a newer impact projection.
- The handler compares an impact revision only with its local impact
  watermark. An equal or lower revision is ignored; a greater revision is
  handled once, even when a newer generic canonical snapshot has arrived.
- Payment must publish the impact only after its authoritative transaction
  update commits.
- Do not enable the impact flow until both services have deployed their
  compatible contract changes and the receiving Kafka binding has been added
  to `idpay-transactions`.

## PR update checklist

For every related PR, update the status table and this checklist:

- [ ] Payment transaction storage/model persists `transactionRevision`.
- [ ] Payment increments it atomically for every published canonical change.
- [ ] Generic transaction snapshots include it.
- [ ] Payment outbox publishes the dedicated impact with stable retries.
- [ ] Payment invokes the read-only eligibility query before the command.
- [ ] Both repositories have contract, ordering, retry, and error-path tests.
- [ ] `idpay-transactions` has its production consumer binding and deployment
  configuration.
- [ ] Deployment/cutover enablement is agreed after both sides are compatible.

## Change log

| PR | Change |
| --- | --- |
| `idpay-transactions` PR 20 (`a3a6f73`) | Added the local SQL projection, revision-aware generic synchronization, eligibility port, and contract model. It does not add a payment producer or a runtime consumer binding. |
| `idpay-transactions` PR 20 watermark follow-up | Consolidated the never-deployed migration into the final transaction-local impact watermark schema; no inbox table is created. |
