# Permanent transaction failures: minimal implementation plan

## Goal

Process explicit permanent transaction failures once per delivery, without local
retries, and send the original message to `idpay-errors` with `retryable=false`.
Keep existing acknowledgment behavior and retry handling for all other failures.

## Scope

Permanent failures covered by this change:

- Invalid JSON or incompatible input field types during deserialization.
- Missing, empty, blank or multiple transaction initiatives.
- An incoming transaction attempting to change an existing transaction's initiative.

Do not classify generic `IllegalArgumentException`, `IllegalStateException` or all
database errors as permanent. Do not convert `initiativeId` into `initiatives`
or otherwise change the accepted event contract.

## Changes

### 1. Add one explicit exception type

Add `PermanentMessageException extends RuntimeException` in
`it.gov.pagopa.common.reactive.kafka.exception`, with message and cause
constructors.

Use it only at known permanent-failure sites:

- `Utilities.requireInitiativeId`: validate initiative cardinality/content,
  including a null list element. Reuse this check from the SQL mapper and
  from the transaction mediator before invoking the service. This earlier
  validation also covers `INVOICED` events, which access the initiative before
  SQL mapping, without changing the accepted event contract.
- `SqlRewardTransactionAdapter.completeUpsert`: replace only the
  `IllegalStateException` reporting that an existing transaction belongs to a
  different initiative.

Retain the existing SQL guards and transaction rollback behavior.

### 2. Add a retry-policy hook to the base consumer

In `BaseKafkaConsumer`, extract the current retry predicate into:

```java
protected boolean isRetryable(Throwable error) {
    return !(error instanceof UncommittableError);
}
```

Use `.filter(this::isRetryable)` in the existing `Retry.backoff` configuration.
The default keeps all other consumers unchanged.

In `PersistenceTransactionMediatorImpl`, override the hook to also reject
`PermanentMessageException`. Inspect its cause chain so an explicitly typed
failure remains permanent when wrapped; do not match exception message text.
Reuse that permanent-failure check when choosing the notification flag.

### 3. Mark transaction error notifications correctly

In `PersistenceTransactionMediatorImpl.notifyError`, pass `retryable=false`
for `PermanentMessageException`, including wrapped instances. Keep
`retryable=true` for other processing failures.

In `onDeserializationError`, change the existing notification's retryable
argument to `false`. Deserialization already uses a callback and returns no
payload, so it does not enter the local retry path. Preserve that behavior;
do not introduce a second error notification or a new deserialization pipeline.

No notifier-interface, producer, topic or schema changes are needed.

## Expected behavior

| Outcome | Processing attempts per delivery | Error notification |
| --- | --- | --- |
| Valid transaction | One, unless a retryable failure occurs | None on success |
| Explicit permanent processing failure | One | Once, `retryable=false` |
| Invalid input JSON/field type | One deserialization attempt | Once, `retryable=false` |
| Other processing failure | Existing initial attempt plus up to three retries | Existing behavior, `retryable=true` |

After notification, preserve the current timed acknowledgment path so a
permanent record does not prevent subsequent records from being processed.

## Focused regression tests

- Feed the incident payload containing singular `initiativeId`: verify one
  processing attempt and one notification with `retryable=false`.
- Cover null, empty, blank, null-element and multiple-initiative lists.
- Verify invalid initiatives on `INVOICED` events are rejected before the
  service's batch-enrichment path, without retries or persistence calls.
- Verify a conflicting initiative produces the permanent exception and leaves
  the stored transaction and its batch counters unchanged.
- Cover direct and wrapped permanent failures in the retry predicate and
  notification classification.
- Verify invalid JSON and incompatible field types notify once with
  `retryable=false`, without invoking persistence.
- Verify transient/unknown failures retain the existing retry count and
  `retryable=true`; `UncommittableError` retains its existing handling.
- Verify a valid record after a permanent failure is processed and acknowledged.
- Verify other base-consumer subclasses retain their existing retry policy.

Run the targeted tests and the repository's Java quality gate when implementing.

## Operational prerequisite and non-goals

The error-topic replayer must honor `retryable=false`. This change prevents
local retries and marks new error records as non-replayable; it cannot stop a
replayer that ignores the flag or remove previously published copies.

Publication-confirmation guarantees, notifier failure propagation, replay
attempt limits, dedicated quarantine storage, metrics and payload repair are
outside this minimal change.
