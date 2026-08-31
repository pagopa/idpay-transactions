# Design Review: Targeted SENT Reward-Batch Evaluation

## Goal

Allow tests and internal operators to evaluate specific `SENT` reward batches without transitioning every `SENT` batch belonging to the initiative.

The endpoint is internal-only and is reached through the cluster ingress from the VPN. It does not require application-level authentication or authorization.

Backward compatibility is the primary constraint. Existing request payloads, selection semantics, and successful HTTP responses must remain unchanged.

## API

Keep the existing endpoint:

```http
POST /idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/evaluate
Content-Type: application/json
```

Targeted request:

```json
{
  "rewardBatchIds": ["batch-id-1", "batch-id-2"]
}
```

Cron-compatible request:

```json
{}
```

Semantics:

- Missing or `null` `rewardBatchIds`: evaluate every `SENT` batch for the initiative.
- Non-empty `rewardBatchIds`: evaluate only the listed batches.
- Empty array: perform no evaluation and return success, preserving the existing behavior.

Successful processing returns `200 OK`, preserving the current endpoint contract.

An absent or malformed JSON body continues to use Spring's existing request-binding error behavior. It is not treated as cron mode; cron mode requires a valid object such as `{}`.

## Validation

- Normalize the request once, before logging or database access.
- Remove duplicate IDs while preserving their first-occurrence order.
- Ignore `null` or blank entries, preserving the endpoint's lenient internal-input behavior and avoiding null-sensitive sanitization or lookup calls.
- A requested batch is eligible only if it belongs to `initiativeId` and is in `SENT`.
- Missing batches, batches from another initiative, and batches not in `SENT` are skipped, preserving the existing successful response contract.

Validation must not broaden the selection: an invalid requested ID must never cause evaluation to fall back to all `SENT` batches.

The implementation must preserve whether the original field was `null`:

- `rewardBatchIds == null` selects all `SENT` batches.
- `rewardBatchIds != null` always remains targeted mode, even if normalization produces an empty list.

This distinction prevents `[]`, `[null]`, blank-only lists, or fully invalid lists from accidentally triggering initiative-wide evaluation.

## Processing

Reuse the existing single-batch evaluation operation:

1. Lock the selected batch in `SENT`.
2. Update its transactions according to evaluation rules.
3. Move the batch to `EVALUATING`.

Normalize targeted requests by removing invalid entries and duplicates while preserving first-occurrence order. Process the remaining IDs sequentially with `concatMap`.

Each batch transition remains an independent transaction. The `SENT` row lock in the single-batch operation is the authoritative eligibility check and atomic claim. If the batch is absent from that lock query because it is missing, belongs to another initiative, or is no longer `SENT`, complete that batch operation empty and skip it.

A failure after an eligible batch has been claimed rolls back that batch transaction and terminates the request. Batches committed earlier in the sequence remain committed. Request-wide atomicity is deliberately not introduced because it would change existing failure semantics and require holding locks across the complete selection.

For the all-batches mode, preserve the existing query and processing behavior. This design does not introduce request-wide atomicity because doing so would change runtime and failure semantics and require locking an unbounded initiative-wide selection.

### Targeted lookup

Avoid loading a complete aggregate once per requested ID only to discover eligibility. Add a lightweight persistence operation that selects eligible identifiers:

```sql
SELECT id
FROM reward_batches
WHERE initiative_id = :initiativeId
  AND status = 'SENT'
  AND id IN (:rewardBatchIds);
```

Use the result only as a prefilter. The transactional lock remains mandatory because eligibility can change between this query and processing.

Restore first-occurrence request order before applying `concatMap`; SQL `IN` queries do not preserve input order. For large internal requests, execute the lookup in bounded chunks rather than adding a new request-size rejection that could break compatibility.

### Concurrency

Concurrent cron and targeted requests may select the same batch. Correctness relies on the conditional `SENT` lock and update, not on the prefilter:

1. The first transaction locks and transitions the batch.
2. A concurrent transaction sees no eligible `SENT` row after the first commit.
3. The concurrent attempt completes empty and is counted as skipped.

The operation is therefore at-most-once for the `SENT -> EVALUATING` transition without making retries fail.

### Failure semantics

| Condition | Behaviour |
| --- | --- |
| Empty targeted selection | `200 OK`, no-op |
| Missing batch | Skip |
| Batch in another initiative | Skip |
| Batch not in `SENT` | Skip |
| Batch concurrently transitioned | Skip |
| Database or evaluation error | Propagate the error; return the existing mapped `5xx` response |

No new `4xx` response is introduced for a previously accepted targeted request.

## Observability

Application-level authentication is not required because access is restricted by cluster ingress and VPN. The implementation should still provide operational traceability.

Emit one structured completion log containing:

- sanitized initiative ID;
- requested, normalized, eligible, processed, and skipped counts;
- a bounded, sanitized sample of requested batch IDs;
- the existing request or correlation ID when available.

Do not log an unbounded identifier list. Do not expose whether a skipped ID was missing or belonged to another initiative; both use the same skipped outcome.

## Compatibility

- Do not change the cron request in `evaluate_sent_reward_batch`; it continues sending `{}`.
- Do not add a second endpoint.
- Preserve `200 OK` for successful requests.
- Preserve an empty array as a successful no-op.
- Preserve lenient handling of unknown, cross-initiative, and non-`SENT` targeted IDs.
- Never interpret an explicit non-null ID list, including an empty or fully invalid list, as “evaluate all”.
- Document `RewardBatchesRequest.rewardBatchIds` in the internal API contract.
- Do not add an authentication header requirement.
- Do not add a request-size rejection; use bounded lookup chunks instead.
- Do not change the all-batches query, ordering, or failure behavior as part of this change.

## Tests

1. `{}` evaluates all `SENT` batches for the initiative.
2. `{"rewardBatchIds": null}` evaluates all `SENT` batches for the initiative.
3. One ID evaluates only that batch.
4. Multiple IDs evaluate only the selected batches.
5. Duplicate IDs are processed once.
6. An empty array is a successful no-op.
7. Null and blank entries are ignored without broadening the selection.
8. A missing batch is skipped.
9. A batch from another initiative is skipped.
10. A batch not in `SENT` is skipped.
11. A mixed list evaluates only its eligible requested batches.
12. A fully invalid non-empty list is a successful no-op and does not evaluate all batches.
13. Successful requests return `200 OK`.
14. Concurrent attempts evaluate a batch at most once.
15. An evaluation failure is propagated without rolling back previously committed batches.
16. A malformed or absent request body retains the existing Spring request-binding response.
17. Targeted IDs are processed in first-occurrence request order.
18. A targeted lookup result is revalidated by the transactional `SENT` lock.
19. A large targeted request is looked up in bounded chunks without changing its result.
20. Completion logging reports bounded identifiers and the expected processing counts.

## Acceptance criteria

- Functional tests can evaluate their own batch by ID.
- Unselected `SENT` batches remain unchanged.
- Existing cron behavior remains backward compatible.
- Every previously successful request shape remains successful with `200 OK`.
- Empty or invalid targeted selections can never trigger initiative-wide evaluation.
- Each batch transition remains transactional and concurrency-safe.
- Concurrent cron and targeted executions cannot evaluate the same batch twice.
- Operational failures remain visible and are not converted into successful skips.
- Targeted processing avoids an aggregate-query N+1 pattern.
