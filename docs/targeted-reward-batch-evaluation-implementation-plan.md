# Implementation Plan: Targeted SENT Reward-Batch Evaluation

## Objective

Implement targeted evaluation of `SENT` reward batches while preserving every existing successful request shape and response:

- `{}` and `{"rewardBatchIds": null}` evaluate all `SENT` batches for the initiative.
- A non-null list is always targeted mode.
- `[]`, null-only, blank-only, or fully ineligible lists are successful no-ops.
- Successful requests return `200 OK` with an empty body.
- Missing, cross-initiative, non-`SENT`, and concurrently transitioned batches are skipped.
- Database or evaluation failures remain errors.
- A targeted request must never fall back to initiative-wide evaluation.

The endpoint is internal-only, reached through the cluster ingress from the VPN, and does not require application-level authentication.

The detailed design is in
[`targeted-reward-batch-evaluation-design-review.md`](targeted-reward-batch-evaluation-design-review.md).

## Instructions for the coding agent

- Execute the tasks in dependency order and keep each task independently mergeable and deployable.
- Do not combine all tasks into one pull request unless explicitly requested.
- Before each task, inspect the current implementation and tests because earlier tasks may already have changed the listed files.
- Preserve the existing endpoint path, request DTO, empty response body, and `200 OK` success status.
- Do not add authentication or authorization requirements.
- Do not add a request-size rejection.
- Do not change the all-batches query, ordering, or failure behavior.
- Do not introduce request-wide transactions or locks across multiple batches.
- Keep the single-batch `SENT` lock as the authoritative atomic eligibility check.
- Follow `.github/instructions/java-quality.instructions.md`.
- For every Java change, run the `java-quality-gate` workflow before declaring the task complete.
- Do not commit generated or unrelated files.

## Deployment sequence

```text
Task 1 -> Task 2 -> Task 3
             |
             +----> Task 4
```

Task 1 establishes the compatibility baseline. Task 2 is the functional implementation. Task 3 is a behavior-preserving query optimization. Task 4 may start after Task 2 and does not depend on Task 3.

Do not split Task 2 further. Mode detection, normalization, ordered processing, and concurrency-safe skipping must be deployed together. An intermediate implementation could otherwise convert an empty normalized targeted selection into all-batches mode.

## Task 1: Characterize the compatibility contract

### Goal

Lock the current public behavior in tests before changing production code.

### Expected production impact

None. This task changes tests only.

### Candidate files

- `src/test/java/it/gov/pagopa/idpay/transactions/controller/MerchantRewardBatchControllerImplTest.java`
- `src/test/java/it/gov/pagopa/idpay/transactions/service/RewardBatchServiceImplTest.java`
- `src/test/java/it/gov/pagopa/idpay/transactions/persistence/sql/SqlRewardBatchEvaluationAdapterTest.java`

### Work

1. Add controller tests proving that successful evaluation returns `200 OK` with an empty body.
2. Characterize `{}` and `{"rewardBatchIds": null}` as all-batches mode.
3. Characterize `{"rewardBatchIds": []}` as a successful no-op.
4. Record the existing Spring response for an absent or malformed request body without changing it.
5. Add service tests that distinguish a null list from a non-null empty list.
6. Add or retain SQL integration coverage proving that concurrent calls transition a batch at most once.

If a desired compatibility behavior is not currently implemented, write the test as disabled with a precise reason and enable it in Task 2. Do not weaken assertions to match accidental behavior.

### Acceptance criteria

- Existing successful status and payload semantics are explicit in tests.
- Tests clearly distinguish all-batches mode from targeted no-op mode.
- No production file is changed.
- The existing test suite remains green.

### Deployment

Merge safely before Task 2. There is no runtime behavior change.

## Task 2: Implement safe targeted orchestration

### Goal

Implement the complete targeted behavior without changing the all-batches or HTTP contracts.

### Candidate files

- `src/main/java/it/gov/pagopa/idpay/transactions/controller/MerchantRewardBatchController.java`
- `src/main/java/it/gov/pagopa/idpay/transactions/controller/MerchantRewardBatchControllerImpl.java`
- `src/main/java/it/gov/pagopa/idpay/transactions/dto/RewardBatchesRequest.java`
- `src/main/java/it/gov/pagopa/idpay/transactions/service/RewardBatchService.java`
- `src/main/java/it/gov/pagopa/idpay/transactions/service/RewardBatchServiceImpl.java`
- `src/main/java/it/gov/pagopa/idpay/transactions/persistence/sql/SqlRewardBatchEvaluationAdapter.java`
- Corresponding controller, service, and SQL integration tests

### Required design

Preserve mode before normalization:

```text
rewardBatchIds == null     -> all-batches mode
rewardBatchIds != null     -> targeted mode
```

For targeted mode:

1. Remove null and blank entries before sanitization or lookup.
2. Remove duplicates while preserving first-occurrence order.
3. Keep an empty normalized list in targeted mode and complete successfully.
4. Process normalized IDs sequentially with `concatMap`.
5. For each ID, invoke the existing transactional single-batch evaluation operation.
6. Treat an empty single-batch result as skipped.
7. Propagate operational errors; do not convert them into skips or successful fallbacks.

The transactional SQL operation must:

1. Select and lock the batch using ID, initiative ID, and `SENT` status.
2. Return empty if no eligible row is available.
3. Update transaction evaluation state and transition the batch within the same transaction.
4. Roll back that batch transaction on an update failure.

Do not prevalidate the complete request and then assume it remains valid. Eligibility can change concurrently; the transactional lock is authoritative.

### Required tests

- One targeted ID evaluates only that batch.
- Multiple targeted IDs evaluate only those batches.
- Duplicates execute once in first-occurrence order.
- Empty, null-only, and blank-only targeted lists are successful no-ops.
- A fully invalid targeted list does not query or evaluate all batches.
- Missing, cross-initiative, and non-`SENT` IDs are skipped.
- A mixed list evaluates only eligible requested batches.
- Concurrent cron and targeted attempts transition a batch at most once.
- A database or evaluation error terminates processing.
- Batches committed before a later failure remain committed.
- All successful variants return `200 OK`.
- `{}` and a null field continue through the original all-batches path.

### Acceptance criteria

- Every compatibility test from Task 1 passes.
- Explicit targeted mode can never call the all-batches selection as a fallback.
- Evaluation remains transactional per batch.
- Concurrent execution cannot evaluate a batch twice.
- No new `4xx` response is introduced for previously accepted targeted requests.
- Operational failures remain visible through the existing error mapping.

### Deployment

This is the functional release. It must be deployable independently of Tasks 3 and 4.

## Task 3: Optimize targeted eligibility lookup

### Goal

Remove the aggregate-query N+1 pattern without changing Task 2 behavior.

### Candidate files

- `src/main/java/it/gov/pagopa/idpay/transactions/persistence/port/RewardBatchLifecyclePort.java`, or a narrower dedicated read port
- `src/main/java/it/gov/pagopa/idpay/transactions/persistence/sql/SqlRewardBatchLifecycleAdapter.java`
- `src/main/java/it/gov/pagopa/idpay/transactions/persistence/sql/SqlRewardBatchListAdapter.java`, or a dedicated lightweight adapter
- `src/main/java/it/gov/pagopa/idpay/transactions/service/RewardBatchServiceImpl.java`
- Corresponding service and SQL integration tests

Prefer a narrow persistence method returning IDs rather than loading projected `RewardBatch` aggregates:

```sql
SELECT id
FROM reward_batches
WHERE initiative_id = :initiativeId
  AND status = 'SENT'
  AND id IN (:rewardBatchIds);
```

### Work

1. Add a lightweight method that returns eligible IDs for an initiative and requested ID collection.
2. Split large ID collections into bounded internal chunks. The chunk size is an implementation/configuration detail, not an API limit.
3. Combine chunk results without duplicates.
4. Restore normalized request order before processing because SQL `IN` does not preserve input order.
5. Continue calling the transactional single-batch operation for every prefiltered ID.
6. Do not remove the transactional eligibility recheck.

### Required tests

- The persistence query returns only matching `SENT` IDs for the initiative.
- Result order from the database does not affect processing order.
- Requests spanning multiple lookup chunks produce the same result as a single chunk.
- A batch that becomes ineligible after prefiltering is skipped by the transactional lock.
- Task 2 behavior tests remain unchanged and green.

### Acceptance criteria

- Targeted selection does not load one aggregate per requested ID.
- Chunking does not reject or truncate a request.
- Processing order remains first-occurrence request order.
- The prefilter is never treated as the concurrency guarantee.
- No HTTP or lifecycle behavior changes.

### Deployment

Deploy independently after Task 2. Rollback must restore only the less-efficient lookup, not change endpoint behavior.

## Task 4: Add bounded observability and contract documentation

### Goal

Make internal executions diagnosable without changing selection or lifecycle behavior.

### Candidate files

- `src/main/java/it/gov/pagopa/idpay/transactions/controller/MerchantRewardBatchControllerImpl.java`
- `src/main/java/it/gov/pagopa/idpay/transactions/service/RewardBatchServiceImpl.java`
- Existing internal API contract or endpoint documentation, if present
- Corresponding tests

### Work

1. Produce a processing summary containing requested, normalized, eligible, processed, and skipped counts.
2. Emit one structured completion log per request.
3. Include the sanitized initiative ID and existing request/correlation ID when available.
4. Log only a bounded, sanitized sample of requested batch IDs.
5. Do not distinguish a missing ID from a cross-initiative ID in externally visible output.
6. Document `RewardBatchesRequest.rewardBatchIds` semantics in the authoritative internal contract.
7. If no checked-in API contract exists, document the Java endpoint/DTO using the repository's existing documentation conventions rather than introducing a new specification framework.

Do not add authentication headers, response fields, or a new endpoint.

### Required tests

- Completion counts are correct for all-valid, mixed, and no-op targeted requests.
- Duplicate and invalid entries do not inflate processed counts.
- Concurrently transitioned batches are counted as skipped.
- Logged ID collections are bounded and sanitized.
- Logging does not alter the returned status or error.

### Acceptance criteria

- Operators can determine how many batches were requested, processed, and skipped.
- Logs do not contain unbounded lists.
- The internal contract documents null, empty, targeted, and all-batches semantics.
- No HTTP request or response shape changes.

### Deployment

Deploy independently after Task 2, before or after Task 3.

## Final end-to-end checks

After all tasks are merged:

1. Send `{}` and confirm the cron-compatible all-batches behavior.
2. Send one known `SENT` batch ID and confirm only that batch transitions.
3. Send a mixture of eligible and ineligible IDs and confirm only eligible IDs transition.
4. Send `[]` and confirm `200 OK` with no transitions.
5. Run concurrent all-batches and targeted requests against the same batch and confirm one transition.
6. Confirm operational failures are returned through the existing error mapping.
7. Confirm logs contain bounded identifiers and accurate counts.

The implementation is complete only when the targeted endpoint is safe to retry, cannot broaden an explicit selection, and remains backward compatible with the existing cron and internal callers.
