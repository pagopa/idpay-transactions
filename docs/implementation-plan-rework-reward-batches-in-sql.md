# Implementation Plan: Reactive PostgreSQL Reward-Batch Rework

## Scope

Replace MongoDB persistence of `RewardBatch` and `RewardTransaction` with PostgreSQL through R2DBC while preserving current API, Kafka, Azure Blob, and REST integration contracts. The final code PR directly replaces Mongo; an external process will have already migrated the data.

Excluded: Mongo-to-PostgreSQL extraction/backfill, reconciliation execution, dual reads/writes, feature flags, and data cutover operations.

## Schema-management decision

The application must **not** include Flyway, a JDBC migration driver, or any other in-service schema migration mechanism.

Versioned, ordered SQL migration files remain in `src/main/resources/db/migration/` as the canonical schema artifact. A future external schema-management mechanism, such as an init container, applies those files before the application starts. The application accesses PostgreSQL only through R2DBC.

Testcontainers integration tests may apply the repository migration files solely to provision isolated test databases; this is not application startup behavior and must not create a schema-history table.

## Current state

- Spring Boot 4.0.2 / Java 25, WebFlux, and reactive MongoDB.
- `RewardBatch` is stored in `rewards_batch`; `RewardTransaction` is stored in `transaction`.
- Mongo-specific repositories and templates are called directly by batch, transaction, POS, invoice, and lifecycle services.
- Invoice update/reversal, suspended reassignment, and merchant postponement change transaction membership/state and require SQL transaction boundaries.
- No R2DBC, PostgreSQL, or SQL migration-file test fixture currently exists.

## Architecture rules

1. Use Spring Data R2DBC, `DatabaseClient`/custom SQL, and `TransactionalOperator`; do not add JPA, Hibernate, blocking repositories, or `.block()` in application flows.
2. Preserve controllers, DTOs, error codes, Kafka contracts, Blob paths, and external REST integrations.
3. Keep storage-specific annotations and query code in persistence adapters behind ports.
4. Persist one non-null `initiative_id` per transaction. A nullable `reward_batch_id` references a batch of the same initiative through a composite foreign key.
5. Persist deferred complex structures as JSONB initially; retain typed/indexed columns for all batch, search, and lifecycle predicates.
6. Do not persist mutable batch counters. Derive all batch amount/count response fields through database-side aggregate queries over assigned transactions; do not aggregate in application memory or introduce N+1 reads.
7. Persist `accrued_reward_cents` as a typed transaction column for aggregate queries. Persist an Erogazioni amount only as an immutable delivery-request snapshot or outbox payload.
8. Intermediate PRs retain Mongo behavior through adapters. The final PR selects SQL and removes Mongo; no runtime dual-write/read exists.

## Thin PR sequence

| PR | Deliverable | Depends on |
| --- | --- | --- |
| 01 | Add PostgreSQL R2DBC and Testcontainers PostgreSQL dependencies. Add only `spring.r2dbc` configuration and document the external schema-management contract and PostgreSQL environment variables. Do not add Flyway, a JDBC driver, or schema-startup configuration. | — |
| 02 | Add `001-create-reward-batches.sql` under `src/main/resources/db/migration/`, defining `reward_batches`, current fields, non-negative counter constraints, grouping uniqueness, and list/delivery/outcome indexes. Add a test fixture that provisions PostgreSQL from repository migration files. | 01 |
| 03 | Add `002-create-reward-transactions.sql`, defining typed identity/search/batch fields, JSONB deferred structures, `initiative_id NOT NULL`, composite batch/initiative FK, and lookup/batch-status/sampling indexes. Add JSONB converter tests. | 02 |
| 04 | Add the next ordered SQL migration with read-only batch-counter reconciliation views/queries. Document expected results; do not execute reconciliation or backfill. | 03 |
| 05 | Add the next forward-only migration: drop the temporary reconciliation views and persisted batch counter columns, add typed `accrued_reward_cents`, and add the aggregate-query indexes/constraints. Document the external legacy-counter-to-SQL-aggregate audit; do not execute it. | 04 |
| 06 | Introduce batch read/write, transaction read/write, and atomic-mutation ports. Provide behavior-preserving Mongo adapters and migrate the batch lookup use case. | 01 |
| 07 | Put batch list/count filtering, `TO_APPROVE`/`TO_WORK` translation, pagination, sorting, and prior-month lookup behind the Mongo batch adapter; characterize current authorization-visible results. | 06 |
| 08 | Put transaction search/count/filter behavior behind the Mongo transaction adapter, including fiscal-code/POS/product/trx-code filters, status ordering, and `TO_CHECK` visibility. | 06 |
| 09 | Put Kafka save/upsert, assignment candidate lookup, transaction-in-batch reads, and invoice lookups behind the Mongo transaction adapter; preserve `REFUNDED` skipping and `INVOICED` payment cancellation. | 08 |
| 10 | Put batch lifecycle reads and simple status/metadata updates behind ports without changing state rules. | 07 |
| 11 | Put transaction decisions behind an atomic-mutation port. Keep Mongo behavior and add table-driven tests for every old/new in-batch-state aggregate result. | 09, 10 |
| 12 | Put invoice update/reversal, suspended reassignment, and postponement behind explicit mutation commands with characterization tests. | 11 |
| 13 | Remove direct `ReactiveMongoTemplate` use from `RewardBatchServiceImpl`, representing cleanup as a port operation. | 10 |
| 14 | Implement SQL batch records/mappers and basic SQL batch adapter CRUD, unique create-or-read, identity access, and simple status/metadata writes. Test duplicate-key races. | 02, 05, 06 |
| 15 | Implement SQL batch lists/counts with database-side aggregate projections, virtual statuses, ordering, delivery/outcome selection, prior-month validation, and empty-batch eligibility. | 14 |
| 16 | Implement SQL transaction records, JSONB converters, typed accrued reward, and initiative-safe idempotent upsert. Reject changes to an existing transaction's initiative. | 03, 05, 09 |
| 17 | Implement SQL transaction searches, database paging/sorting, batch lists, invoice lookup, and deterministic sampling. | 16 |
| 18 | Implement atomic batch assignment: lock/find-or-create batch, claim one eligible transaction, and set assignment fields exactly once. Test retry and concurrency. | 14, 16, 17 |
| 19 | Implement evaluation preparation and decision mutations with conditional transaction updates, idempotency, batch locks, and aggregate-projection tests. | 11, 14, 16 |
| 20 | Implement transactional invoice update/reversal and lifecycle reads, preserving Basic/Full policies and state/membership invariants. | 12, 19 |
| 21 | Implement final-approval suspended reassignment: stable source/target locks, target creation, last-elaborated-month preservation, `INVOICED`/`SUSPENDED` state, and full invariant tests. | 12, 19 |
| 22 | Implement constrained merchant postponement: `CREATED` source only, initiative end-date validation, and `CREATED` target. | 12, 19 |
| 23 | Route approval worker, assignee promotion, CSV source queries, delivery amount snapshot/outcome updates, and empty-batch cleanup through SQL-capable ports. | 15, 17, 19, 21, 22 |
| 24 | Add PostgreSQL integration coverage for consumer idempotency, assignment, lifecycle transitions, aggregate projections, moves, invoice policies, CSV selection, delivery amount snapshots/outcomes, and external reconciliation input queries. | 18–23 |
| 25 | Direct cutover: select SQL adapters; remove Mongo repositories/models/configuration/health/dependencies/embedded-Mongo tests; update deployment configuration. Do not add data transfer, feature flags, or dual writes. | 23, 24 |
| 26 | Run regression and prove ordered repository migration files provision an empty PostgreSQL database through the test fixture, contracts remain compatible, Kafka behavior is unchanged, and no Mongo runtime dependency remains. Update the decision record and external schema-management hand-off checklist. | 25 |

## PR execution rules

- Keep every PR limited to its stated deliverable.
- Every PR must build successfully and pass the full test suite.
- Commit the changes to a git branch from `migration-to-sql-database` base branch. Do not use git branches with folder separator like `feat/example`. 
- Use `StepVerifier` for reactive unit tests and focused Testcontainers PostgreSQL integration tests for SQL adapter/mutation work.
- Apply SQL mutations only through `TransactionalOperator`; never add blocking bridges.
- Before merging a mutation PR, assert the transaction, all affected batch aggregate projections, and idempotent retry/concurrent behavior where applicable.
- Treat `src/main/resources/db/migration/` as ordered, forward-only artifacts. The application must neither execute nor track them.
- Keep data backfill, production reconciliation execution, and rollback of migrated data outside this work.

## Cutover hand-off conditions

- Before PR 25 deploys, the external process must populate PostgreSQL and produce a clean external comparison of legacy Mongo counters against SQL transaction aggregates.
- It must retain string IDs and JSON payload fidelity, satisfy foreign keys, populate typed accrued reward, and populate all fields used by lists, invoices, CSV, delivery, and refunds.
- Transactions with multiple Mongo initiatives must be quarantined/remediated externally; SQL must not choose an initiative arbitrarily.
- The deployment mechanism must apply the ordered schema migration files before the cutover application starts. The service intentionally contains no Flyway dependency, JDBC migration connection, or migration history table.
