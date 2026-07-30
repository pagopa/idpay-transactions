# DR: Extend `updateInvoiceFile` to replace invoice for EXCLUDED/REJECTED transactions

Date: 2026-03-04

Author: AI Agent (for implementer)

## Status

Superseded by the payment-to-reward-batch-impact boundary in
`docs/DR-rework-reward-batches-in-sql.md`. The historical instructions below
must not be used to add invoice/reversal endpoints, blob operations, mutable
batch counters, or payment-state ownership to this service.

## Summary
- The existing endpoint `PUT /idpay/transactions/{transactionId}/invoice/update` (controller: `PointOfSaleTransactionController#updateInvoiceFile`) must be extended so merchants / points of sale can replace invoice documents for transactions in `REJECTED` (aka EXCLUDED) state, including when the transaction's batch is in `APPROVED` state. Behavior must match the portal "weak-credentials" flow: accept PDF or XML, same validations and audit fields.

## Goals / Acceptance Criteria
- The existing `updateInvoiceFile` endpoint (used from point-of-sale detail and also from batch detail) continues to be used; no new endpoint required.
- Allow replacing invoice files for transactions whose status is `REJECTED` (EXCLUDED) and for transactions in batches with status `APPROVED` or `EVALUATING` (per NOTE).
- Only `application/pdf` and `application/xml` (or equivalent) accepted; same size and filename rules as portal weak-credentials.
- After a successful replacement:
  - The transaction status MUST be set to `SUSPENDED`.
  - The transaction MUST be removed from its old batch and assigned to the next batch (or a newly created batch) following existing batch assignment rules.
  - Old invoice document MUST be deleted from storage (Azure Blob) after successful upload and DB update (or via robust compensation if deletion fails).
  - Update batch counters exactly as specified:
    - In the old batch (always): decrement `numberOfTransactions` and `initialAmountCents`.
    - If the transaction previous state contributes to `trxElaborated` counters (REJECTED/ SUSPENDED semantics): decrement `trxElaborated` and the appropriate specific counter(s) (`trxRejected` or `trxSuspended`) and `suspendedAmountCents` if applicable.
    - In the destination batch: increment `initialAmountCents`, `numberOfTransactions`, `trxSuspended`, `suspendedAmountCents`, and `trxElaborated`.
  - The batch `assignmentLevel` must remain unchanged when moving the transaction.
  - The percent of elaborated transactions for the old batch must be recalculated/decremented when `trxElaborated` is decremented.
- Publish domain event(s) for invoice replacement and transaction movement so downstream systems process the change.

## Design / Implementation Notes (AI-implementer actionable)

### 1) Controller
- No new public route required. Extend `PointOfSaleTransactionController#updateInvoiceFile` behavior to allow the operation when the transaction is `REJECTED` (EXCLUDED).
- Ensure Swagger annotations remain correct and that the endpoint continues to accept `multipart/form-data` with parts `file` and optional `docNumber`.

### 2) Service API
- Add/extend service method signature (or reuse existing):
  - `Mono<Void> updateInvoiceFile(String transactionId, String merchantId, String pointOfSaleId, FilePart file, String docNumber)`
- Implementation responsibilities:
  - Validate transaction exists and merchant / PDV ownership.
  - Authorize caller consistent with weak-credentials portal rules.
  - Validate file content-type and size (pdf or xml). If XML, perform well-formedness and schema validation if existing util is present.
  - Upload new file to storage connector (get new blob key/URL).
  - In an atomic operation (Mongo transaction if available, otherwise careful atomic updates + compensation):
    - Read current transaction state and batch id.
    - Persist transaction changes: update invoice metadata (url, filename, contentType), set `status = SUSPENDED`, set `modifiedBy`/`modifiedAt` and update `batchId` to the chosen target batch id.
    - Update old batch counters (decrement `numberOfTransactions`, `initialAmountCents`, and conditionally `trxElaborated` + `trxRejected` or `trxSuspended` / `suspendedAmountCents`). Recompute `elaboratedPercent` if persisted.
    - Determine target batch (existing next batch or create one using existing batch creation rules) and update its counters (increment `initialAmountCents`, `numberOfTransactions`, `trxSuspended`, `suspendedAmountCents`, `trxElaborated`). Preserve `assignmentLevel`.
  - After DB commit, delete old invoice blob from storage. If deletion fails, emit a cleanup event and log the failure.
  - Publish domain events: `transaction.invoice.replaced` and `transaction.moved` (or reuse existing topics/events).

### 3) Batch counter mapping & status translation
- Clarify mapping between domain statuses: treat `EXCLUDED` used in UIs as `REJECTED` in domain counters. Implementation MUST inspect `transaction.status` prior to modification and apply decrements according to that value.
- Example decrements when the previous transaction status is `REJECTED`:
  - `oldBatch.numberOfTransactions -= 1`
  - `oldBatch.initialAmountCents -= tx.initialAmountCents`
  - `oldBatch.trxElaborated -= 1`
  - `oldBatch.trxRejected -= 1`

### 4) Storage connector
- Reuse the existing Azure blob storage connector (`connector/blob` package). Use `upload` method first and `delete` old blob only after DB updates succeed; implement compensating deletion if DB commit fails after upload.

### 5) Persistence and concurrency
- Prefer Mongo multi-document transactions when enabled by the deployment. If not available, use atomic `findAndModify` updates for batch counter increments/decrements (Reactive pattern), combined with optimistic locking on `Transaction` document (`@Version`), or a retry loop.

### 6) Events
- Publish the same event schema used by transaction state changes to avoid breaking downstream consumers. Include `transactionId`, `previousBatchId`, `newBatchId`, `previousStatus`, `newStatus`, `oldInvoiceUrl`, `newInvoiceUrl`, `actor`, and timestamp.

### 7) Tests
- Unit tests (Mockito + StepVerifier):
  - `replaceInvoice_validPdf_forRejectedInApprovedBatch_movesToSuspendedAndUpdatesBatches`.
  - `replaceInvoice_invalidMimeType_returns4xx`.
  - `replaceInvoice_uploadFails_noDbChanges`.
  - `replaceInvoice_dbFails_afterUpload_deletesNewBlob_or_schedulesCleanup`.
- Integration tests:
  - WebTestClient test calling `PUT /idpay/transactions/{transactionId}/invoice/update` with multipart file on a `REJECTED` transaction in an `APPROVED` batch validating all counter updates and blob deletion.

## Files likely to touch
- `src/main/java/it/gov/pagopa/idpay/transactions/controller/PointOfSaleTransactionController.java` (interface - already contains endpoint)
- `src/main/java/it/gov/pagopa/idpay/transactions/controller/PointOfSaleTransactionControllerImpl.java` (implementation)
- `src/main/java/it/gov/pagopa/idpay/transactions/service/TransactionService.java`
- `src/main/java/it/gov/pagopa/idpay/transactions/service/impl/TransactionServiceImpl.java`
- `src/main/java/it/gov/pagopa/idpay/transactions/service/BatchService.java` (or existing batch helper)
- `src/main/java/it/gov/pagopa/idpay/transactions/connector/blob/BlobStorageConnector.java` (reuse)
- `src/main/java/it/gov/pagopa/idpay/transactions/repository/BatchRepository.java`
- `src/test/java/it/gov/pagopa/idpay/transactions/service/TransactionServiceImplTest.java`

## Error handling & compensations
- If file upload fails: return 5xx, no DB change.
- If DB update fails after upload: try to delete newly uploaded blob; if delete fails, log and emit cleanup event.
- If old blob deletion fails after DB commit: log and emit cleanup event but do NOT fail the API call.

## Rollout considerations
- No DB migration required. Behavior is additive to existing endpoint.
- Verify Mongo transaction support in the target environment. If not available, add robust atomic updates and retries.

## Open questions (defaults used)
- Mapping `EXCLUDED` -> treat as `REJECTED` for counter changes.
- Selection for destination batch: reuse existing `BatchAssignmentService.getNextBatchForPointOfSale()`; if none, create a new batch with the same `assignmentLevel` as the source batch.

## Acceptance checklist (QA)
- `PUT /idpay/transactions/{transactionId}/invoice/update` with PDF for a `REJECTED` transaction in an `APPROVED` batch returns 204 and the transaction moves to `SUSPENDED`, old batch counters decremented, new batch counters incremented, and the old blob is deleted.
- Same for XML.
- Non-PDF/XML file is rejected.
- Concurrent replacements handled with optimistic locking.

## Next steps for implementer
- Implement service changes and atomic batch counter updates.
- Add unit tests and integration test.
- Run `./mvnw -Dtest=TransactionServiceImplTest test` and `./mvnw verify`.
