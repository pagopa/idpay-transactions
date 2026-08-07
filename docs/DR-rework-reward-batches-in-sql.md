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
- It retained the current post-invoice cancellation/deletion path, which transfers payment ownership away from `idpay-payment`. The target boundary keeps payment ownership in `idpay-payment`: an `INVOICED` snapshot updates only the local projection and must not cancel or delete the payment transaction.

## Decision

Migrate the batch domain and its local transaction data from reactive MongoDB to PostgreSQL through a reactive SQL stack:

- Use R2DBC and reactive repositories/custom SQL (`Mono`/`Flux`), not JPA, Hibernate, or blocking repositories.
- Use a PostgreSQL schema dedicated to `idpay-transactions`, owned and migrated exclusively by this service. Flyway creates and manages its schema-history table in that schema; it must not baseline or manage a non-empty schema containing tables owned by other services.
- Keep `idpay-transactions` as the physical owner of batches and of the local transaction projection needed to execute batch, reporting, and delivery flows. Payment owns invoice update/reversal commands, their authorization, blob operations, and authoritative transaction-state transitions; this service owns the resulting local reward-batch membership effect. An `INVOICED` snapshot only updates that local projection and must not trigger a cancellation or deletion request to payment.
- Store current batch membership and evaluation state on the local transaction row. The migration must not reduce the local data to a batch-only projection until the consumers of invoice, CSV, POS, and transaction search data have been migrated or replaced.
- Enforce single-initiative ownership: a transaction belongs to exactly one initiative for its whole local lifecycle. It may be assigned to zero or one reward batch at a time; moving or postponing it updates that single assignment and never creates another one.
- Do not persist mutable batch counters. Derive batch amounts and transaction counts in database-side aggregate queries over assigned transactions, and retain their current API response fields as aggregate projections.
- Persist `accrued_reward_cents` as a typed transaction column for aggregation. Keep the source reward payload as JSONB only where it is otherwise required.
- Persist the amount sent to Erogazioni only as immutable delivery-request metadata or in its outbox payload; it is not a mutable batch counter.
- Retain zero-transaction batches throughout their normal lifecycle. They must be available for merchant send and approval under the ordinary state and chronology rules; no endpoint, worker, or persistence adapter deletes them.
- Keep the existing external REST and Kafka contracts unless a separately approved contract change replaces them. The approved exception is a dedicated, versioned payment-to-reward-batch-impact event carrying an event ID, impact type, outcome timestamp, canonical post-operation transaction projection, and shared monotonic transaction revision. Payment also exposes no write coupling here: it calls a narrow read-only eligibility query before committing its command.
- Develop the migration through human-reviewed, fully validated incremental PRs that are not deployed before direct cutover. Introduce a storage port only with the specific caller behavior it represents; do not add generic CRUD or one-to-one repository-wrapper ports solely for a future migration.

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

The transaction identifier is the primary key and `initiative_id` is `NOT NULL`. `reward_batch_id` is a nullable foreign key to `reward_batches`; when present, it must reference a batch with the same initiative. The local projection stores the shared monotonic payment transaction revision and a separate latest-applied-impact revision. Incoming events that associate an existing transaction with a different initiative are a data-integrity error: they must be rejected/quarantined and must not overwrite the existing initiative.

Constraints and indexes:

- primary key on `transaction_id`;
- composite foreign key or equivalent database constraint enforcing that the transaction and its batch share `initiative_id`;
- index on `(reward_batch_id, reward_batch_trx_status)` for evaluation, decision, approval, CSV, and reassignment flows;
- index on `(reward_batch_id, sampling_key)` for sampling queries;
- indexes on transaction identity and invoice lifecycle lookup fields.

### `reports`

Persist report records locally so the existing report generation, list, count,
download, retry, and force-generation APIs remain available after Mongo removal.
Each row retains its string identifier, initiative, report status and type,
period, merchant/business scope, request and elaboration timestamps, operator
scope, and generated filename.

Indexes must support initiative/type/request-date lists and the two mutually
exclusive scopes: merchant reports (`merchant_id` with no operator level) and
operator reports (an operator level).

## Required use cases

| Flow | Required behaviour |
| --- | --- |
| Kafka transaction synchronization | `rewardTrxConsumer` consumes versioned `RewardTransactionDTO` snapshots and conditionally upserts the local projection, preserving existing handling of `REFUNDED` messages. An event may not change an existing transaction's initiative; the conflict is rejected/quarantined. An `INVOICED` snapshot is a local projection update only: it must not cancel or delete the payment transaction, whose ownership remains with `idpay-payment`. |
| Batch assignment | The existing chunked/manual `assignInvoicedTransactionsToBatches` flow finds invoiced, unassigned transactions; enriches missing POS data; finds or creates the `(initiative, merchant, pos type, month)` batch; assigns `CONSULTABLE`; sets inclusion date and deterministic sampling key; and clears prior batch rejection reason. Assignment is idempotent and the transaction's initiative must equal the batch initiative. |
| Batch and processed-transaction reads | Preserve paginated batch list/detail, merchant processed-transaction list/statuses, POS processed-transaction list, batch/status/merchant/POS/fiscal-code/product/trx-code filters, and role-based exposure of `TO_CHECK` as `CONSULTABLE` for non-operators. SQL queries must paginate and sort in the database. |
| Merchant sends a batch | Allow `CREATED -> SENT` only for the owning merchant, only after the batch month, and only when no earlier non-empty `CREATED` batch exists for the same initiative, merchant, and POS type. A zero-transaction batch remains eligible for this and all later ordinary approval transitions. |
| Start evaluation | Select `SENT` batches (explicit IDs or all for the initiative), prepare their assigned transactions for evaluation, and set the batch to `EVALUATING`. Suspended and approved amounts are derived on read. |
| Operator transaction decisions | In `EVALUATING`, approve, reject, or suspend selected transactions. Preserve the current state transitions, rejection reason history, and optional checks-error validation. Repeated decisions must remain idempotent. |
| Assignee promotion | Preserve `L1 -> L2` authorization and the 15% elaboration threshold, then `L2 -> L3` authorization. This only changes the assignee; virtual list states are derived at query time. |
| Final approval | Permit `EVALUATING/L3 -> APPROVING` only when earlier batches for the same merchant, initiative, and POS type are approved or in a refund state. The batch worker then approves remaining `TO_CHECK`/`CONSULTABLE` transactions, moves suspended transactions, sets `APPROVED`, and generates the CSV. This flow also applies when no transactions are assigned. |
| Suspended reassignment on final approval | Move every suspended transaction to the current month or its original future month, creating the target batch when absent. Moved transactions are set to `INVOICED`, and the old elaborated month is recorded if absent. This source/target update must be one SQL transaction. |
| Merchant postponement | From a `CREATED` source batch only, move one selected transaction to the next month. Validate the initiative end-date limit and create/find a `CREATED` target batch. This is distinct from suspended reassignment and must not be generalized into an unrestricted move endpoint. |
| Payment-driven invoice update and invoiced reversal impact | Payment owns the invoice update/reversal command, scope policy, blob operations, and authoritative transaction state. Before committing, payment calls this service's narrow read-only eligibility query. After commit, payment publishes a dedicated impact event keyed by transaction ID with an event ID, schema version, shared transaction revision, outcome timestamp, impact type, and canonical post-operation projection. This service compares the event revision with the transaction-local latest-applied-impact revision: a greater revision applies once, while an equal or lower revision is ignored. An invoice replacement keeps a `CREATED` source membership, otherwise moves the current membership to the outcome-month grouping (creating it when absent) as `SUSPENDED`; a reversal detaches the current membership. The handler applies the impact to current local membership at delivery time, updates no batch counters, and never performs payment/blob work. |
| CSV generation and download | Generate the approved/rejected transaction CSV from local transaction and assignment data, resolve a fiscal code when absent, upload it to Azure Blob Storage, save the filename, and retain signed-URL authorization for merchants and Invitalia operators. |
| Delivery and refund outcomes | Select approved batches whose derived approved amount is positive, resolve merchant/Selfcare data, and persist the immutable amount submitted to Erogazioni with the delivery request. On accepted delivery set `PENDING_REFUND`; poll outcomes and set `REFUNDED` or `NOT_REFUNDED`, including value date or rejection errors. |
| Empty-batch lifecycle | Retain batches with zero transactions. They must follow the ordinary merchant-send and approval lifecycle, and no API, worker, or persistence adapter deletes them. |

## Migration and compatibility requirements

1. Add PostgreSQL and R2DBC dependencies/configuration without introducing blocking database calls.
2. Create a dedicated SQL schema through the project-approved migration mechanism, configure R2DBC and Flyway to use it, and let Flyway create its schema-history table there. Do not use `baselineOnMigrate` to adopt an existing shared schema. The migration must preserve string identifiers unless an explicit cross-service UUID migration is approved.
3. Backfill batches, transactions, and reports, including transaction batch fields and typed accrued reward. Reconcile legacy Mongo counters externally against SQL transaction aggregates; identify transactions with multiple historical initiatives or batch memberships; and quarantine them for remediation before cutover. Do not select an initiative or batch arbitrarily.
4. During dual-read/dual-write or cutover, do not allow Mongo and SQL to independently apply a decision or move. Use an explicit cutover flag, an outbox/idempotency record, or another documented single-writer strategy.
5. Kafka processing must be at-least-once safe: every payment snapshot carries a shared monotonic transaction revision, and every impact carries a unique event ID. Conditional projection updates and the transaction-local latest-applied-impact revision ensure retries or stale generic snapshots cannot duplicate or undo local batch membership.
6. Preserve API response shapes and error semantics until a separately versioned API change is approved.
7. Preserve Azure Blob paths and external Erogazioni, Merchant, User, Initiative, Selfcare, and Payment integrations. Replace only calls whose new owning service and event contract are explicitly documented.
8. Add reactive integration tests for each state transition, database-side aggregate projection, retry/idempotency and revision-ordering case, payment batch impact, reassignment, postponement, delivery amount snapshot, and external reconciliation input query.

## Non-goals and open decisions

- This decision does not move batch ownership to another service. Payment remains the owner of the payment transaction and of invoice update/reversal commands, and publishes the approved dedicated impact event; this service owns only the local batch effect.
- This decision does not create the draft's generic `GET /batches/{batchId}/transactions`, `PUT /batches/{batchId}/approve`, or unrestricted move endpoint. Existing endpoints and authorization rules remain authoritative.
- The relational normalization of `rewards`, invoice structures, rejection reasons, checks errors, and additional properties is deferred. Their required query patterns must be decided before converting JSON/JSONB fields into child tables.
