# Decision Record: migrate reward batches to reactive SQL

## Status

Proposed.

## Context

`idpay-transactions` currently owns the MongoDB `rewards_batch` and `transaction` collections. `RewardBatch` holds the batch lifecycle, persisted reporting metadata, and delivery/refund outcome; `RewardTransaction` holds the complete transaction plus its batch membership and in-batch evaluation state.

The service is Java 25, Spring Boot 4.0.2, WebFlux, reactive MongoDB, and Spring Cloud Stream Kafka. The SQL target must preserve the non-blocking execution model.

The previous draft was inaccurate in the following ways:

- It assumed batch fields are stored by `idpay-payment`; this repository currently stores them in `RewardTransaction`.
- It modelled only a thin batch transaction projection, while current batch operations need local invoice, reward, merchant/POS, CSV, and evaluation data.
- It used states, endpoints, and money types that do not exist here.
- It prescribed JPA/Hibernate and blocking `PagingAndSortingRepository`, which conflict with the reactive service architecture.
- It prohibited all calls to `idpay-payment`, even though the existing Kafka consumer calls it to cancel an invoiced transaction in progress. Only calls intentionally removed by the agreed boundary may be prohibited.

## Decision

Migrate the batch domain and its local transaction data from reactive MongoDB to PostgreSQL through a reactive SQL stack:

- Use R2DBC and reactive repositories/custom SQL (`Mono`/`Flux`), not JPA, Hibernate, or blocking repositories.
- Keep `idpay-transactions` as the physical owner of batches and of the data needed to execute batch, invoice, reporting, and delivery flows.
- Store current batch membership and evaluation state on the local transaction row. The migration must not reduce the local data to a batch-only projection until the consumers of invoice, CSV, POS, and transaction search data have been migrated or replaced.
- Enforce single-initiative ownership: a transaction belongs to exactly one initiative for its whole local lifecycle. It may be assigned to zero or one reward batch at a time; moving or postponing it updates that single assignment and never creates another one.
- Do not persist mutable batch counters. Derive batch amounts and transaction counts in database-side aggregate queries over assigned transactions, and retain their current API response fields as aggregate projections.
- Persist `accrued_reward_cents` as a typed transaction column for aggregation. Keep the source reward payload as JSONB only where it is otherwise required.
- Persist the amount sent to Erogazioni only as immutable delivery-request metadata or in its outbox payload; it is not a mutable batch counter.
- Keep the existing external REST and Kafka contracts unless a separately approved contract change replaces them. In particular, no assumed `PaymentCapturedEvent`, endpoint, or ownership change in another service is introduced by this decision.

This is a data-access and data-model migration. It does not redesign the public API or the business lifecycle.

## Current business model

### Batch identity and fields

A batch is grouped by:

```text
initiativeId + merchantId + posType + month (YYYY-MM)
```

`businessName` is retained with the batch but is not part of the current lookup key. A new batch starts in `CREATED`, has the month start/end dates, a generated Italian month name, and `L1` as assignee.

Batch amount and count response fields are derived from the assigned transactions. Their values remain integer cents where applicable:

- `initialAmountCents`
- `approvedAmountCents`
- `suspendedAmountCents`

The persisted business fields include the assignee level, start/end and lifecycle dates, CSV filename, delivery request amount/outcome, refund value date, refund error, and refund outcome timestamp.

### States

| Scope | Persisted states |
| --- | --- |
| Batch | `CREATED`, `SENT`, `EVALUATING`, `APPROVING`, `APPROVED`, `PENDING_REFUND`, `NOT_REFUNDED`, `REFUNDED` |
| Batch query only | `TO_APPROVE` (`EVALUATING` + `L3`) and `TO_WORK` (`EVALUATING` + `L1` or `L2`) |
| Transaction within a batch | `TO_CHECK`, `CONSULTABLE`, `SUSPENDED`, `APPROVED`, `REJECTED` |

`TO_APPROVE` and `TO_WORK` must remain query translations, not database enum values.

### Derived aggregate invariants

`numberOfTransactions` and `initialAmountCents` describe all transactions currently assigned to a batch. `numberOfTransactionsElaborated`, `numberOfTransactionsSuspended`, `numberOfTransactionsRejected`, `suspendedAmountCents`, and `approvedAmountCents` are derived from the current assigned transaction state and `accrued_reward_cents`, not maintained by transitions.

`approvedAmountCents` is zero before evaluation. From `EVALUATING` onward, it is the accrued reward total of transactions in `TO_CHECK`, `CONSULTABLE`, or `APPROVED`; rejected and suspended transactions are excluded. The elaborated count covers `SUSPENDED`, `APPROVED`, and `REJECTED` transactions.

Each transaction has exactly one `initiative_id` and at most one current `reward_batch_id`. A reassignment updates that column; it must not insert another membership row. The SQL implementation must make each transaction state or membership change atomic and use row locking or conditional updates where concurrent lifecycle rules require it. No batch-counter update accompanies these mutations; aggregates reflect the committed transaction rows.

## Target relational model

Names below are logical. Physical naming must follow the repository conventions adopted with the SQL migration.

### `reward_batches`

One row per `(initiative_id, merchant_id, pos_type, month)` containing:

- identifier, grouping fields, `business_name`, `name`, `partial`, and batch status;
- period, creation, update, merchant-send, approval, and delivery timestamps;
- `assignee_level`;
- CSV/report filename;
- delivery request amount, delivery outcome, and refund outcome fields.

Constraints and indexes:

- primary key on the batch identifier;
- unique constraint on `(initiative_id, merchant_id, pos_type, month)`;
- indexes supporting the current list filters: merchant/initiative, status/initiative, assignee, and month;
- indexes supporting approved-batch delivery selection and outcome polling (`PENDING_REFUND` plus initiative).

### `reward_transactions`

Persist the local transaction representation currently required by this service: identity and synchronization data; its single `initiative_id`; merchant/POS data; transaction and invoice data; reward data; user/fiscal-code data; product/additional properties; transaction status; invoice lifecycle fields; and current batch membership/evaluation state.

Current batch fields are `reward_batch_id` (nullable), `reward_batch_trx_status`, inclusion date, last elaborated month, rejection reasons, checks errors, and sampling key. They replace the Mongo document's corresponding embedded batch fields directly.

The initial migration may retain a JSON/JSONB representation for fields whose relational shape has not yet been designed (for example `rewards`, rejection reasons, invoice data, credit-note data, additional properties, and checks errors). `accrued_reward_cents` is a typed column because batch aggregate queries use it. Queries used by batch flows must have explicit indexed columns rather than relying on unindexed JSON scans.

The transaction identifier is the primary key and `initiative_id` is `NOT NULL`. `reward_batch_id` is a nullable foreign key to `reward_batches`; when present, it must reference a batch with the same initiative. Incoming events that associate an existing transaction with a different initiative are a data-integrity error: they must be rejected/quarantined and must not overwrite the existing initiative.

Constraints and indexes:

- primary key on `transaction_id`;
- composite foreign key or equivalent database constraint enforcing that the transaction and its batch share `initiative_id`;
- index on `(reward_batch_id, reward_batch_trx_status)` for evaluation, decision, approval, CSV, and reassignment flows;
- index on `(reward_batch_id, sampling_key)` for sampling queries;
- indexes on transaction identity and invoice lifecycle lookup fields.

## Required use cases

| Flow | Required behaviour |
| --- | --- |
| Kafka transaction synchronization | `rewardTrxConsumer` consumes `RewardTransactionDTO`, upserts the local transaction idempotently, and preserves existing handling of `REFUNDED` messages. An event may not change an existing transaction's initiative; the conflict is rejected/quarantined. An `INVOICED` transaction still triggers the existing payment cancellation call unless its contract is independently changed. |
| Batch assignment | The existing chunked/manual `assignInvoicedTransactionsToBatches` flow finds invoiced, unassigned transactions; enriches missing POS data; finds or creates the `(initiative, merchant, pos type, month)` batch; assigns `CONSULTABLE`; sets inclusion date and deterministic sampling key; and clears prior batch rejection reason. Assignment is idempotent and the transaction's initiative must equal the batch initiative. |
| Batch and processed-transaction reads | Preserve paginated batch list/detail, merchant processed-transaction list/statuses, POS processed-transaction list, batch/status/merchant/POS/fiscal-code/product/trx-code filters, and role-based exposure of `TO_CHECK` as `CONSULTABLE` for non-operators. SQL queries must paginate and sort in the database. |
| Merchant sends a batch | Allow `CREATED -> SENT` only for the owning merchant, only after the batch month, and only when no earlier non-empty `CREATED` batch exists for the same initiative, merchant, and POS type. |
| Start evaluation | Select `SENT` batches (explicit IDs or all for the initiative), prepare their assigned transactions for evaluation, and set the batch to `EVALUATING`. Suspended and approved amounts are derived on read. |
| Operator transaction decisions | In `EVALUATING`, approve, reject, or suspend selected transactions. Preserve the current state transitions, rejection reason history, and optional checks-error validation. Repeated decisions must remain idempotent. |
| Assignee promotion | Preserve `L1 -> L2` authorization and the 15% elaboration threshold, then `L2 -> L3` authorization. This only changes the assignee; virtual list states are derived at query time. |
| Final approval | Permit `EVALUATING/L3 -> APPROVING` only when earlier batches for the same merchant, initiative, and POS type are approved or in a refund state. The batch worker then approves remaining `TO_CHECK`/`CONSULTABLE` transactions, moves suspended transactions, sets `APPROVED`, and generates the CSV. |
| Suspended reassignment on final approval | Move every suspended transaction to the current month or its original future month, creating the target batch when absent. Moved transactions are set to `INVOICED`, and the old elaborated month is recorded if absent. This source/target update must be one SQL transaction. |
| Merchant postponement | From a `CREATED` source batch only, move one selected transaction to the next month. Validate the initiative end-date limit and create/find a `CREATED` target batch. This is distinct from suspended reassignment and must not be generalized into an unrestricted move endpoint. |
| Invoice update and invoiced reversal | Preserve invoice download, update, and reversal flows. Basic and full invoice lifecycle policies need transaction, in-batch transaction state, and batch-status access; the full policy accepts `CREATED`, `EVALUATING`, `APPROVED`, and refund states, with `CONSULTABLE`, `TO_CHECK`, `SUSPENDED`, or `REJECTED` in-batch transaction state. |
| CSV generation and download | Generate the approved/rejected transaction CSV from local transaction and assignment data, resolve a fiscal code when absent, upload it to Azure Blob Storage, save the filename, and retain signed-URL authorization for merchants and Invitalia operators. |
| Delivery and refund outcomes | Select approved batches whose derived approved amount is positive, resolve merchant/Selfcare data, and persist the immutable amount submitted to Erogazioni with the delivery request. On accepted delivery set `PENDING_REFUND`; poll outcomes and set `REFUNDED` or `NOT_REFUNDED`, including value date or rejection errors. |
| Empty-batch cleanup | Delete only batches with zero transactions before the current month. Implement this as a single predicate-based SQL delete after verifying that no transaction rows reference the batch. |

## Migration and compatibility requirements

1. Add PostgreSQL and R2DBC dependencies/configuration without introducing blocking database calls.
2. Create the SQL schema through the project-approved migration mechanism. The migration must preserve string identifiers unless an explicit cross-service UUID migration is approved.
3. Backfill batches and transactions, including their current batch fields and typed accrued reward. Reconcile legacy Mongo counters externally against SQL transaction aggregates; identify transactions with multiple historical initiatives or batch memberships; and quarantine them for remediation before cutover. Do not select an initiative or batch arbitrarily.
4. During dual-read/dual-write or cutover, do not allow Mongo and SQL to independently apply a decision or move. Use an explicit cutover flag, an outbox/idempotency record, or another documented single-writer strategy.
5. Kafka processing must be at-least-once safe: use transaction identity, event version/timestamp where available, and an inbox/idempotency record so retries cannot duplicate local transactions or batch membership.
6. Preserve API response shapes and error semantics until a separately versioned API change is approved.
7. Preserve Azure Blob paths and external Erogazioni, Merchant, User, Initiative, Selfcare, and Payment integrations. Replace only calls whose new owning service and event contract are explicitly documented.
8. Add reactive integration tests for each state transition, database-side aggregate projection, retry/idempotency case, reassignment, postponement, invoice policy, delivery amount snapshot, and external reconciliation input query.

## Non-goals and open decisions

- This decision does not move transaction ownership to another service. Any future change to `idpay-payment` ownership or its Kafka events requires a separate cross-repository decision record and contract.
- This decision does not create the draft's generic `GET /batches/{batchId}/transactions`, `PUT /batches/{batchId}/approve`, or unrestricted move endpoint. Existing endpoints and authorization rules remain authoritative.
- The relational normalization of `rewards`, invoice structures, rejection reasons, checks errors, and additional properties is deferred. Their required query patterns must be decided before converting JSON/JSONB fields into child tables.
