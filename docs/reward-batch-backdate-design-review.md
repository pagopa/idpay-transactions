# Design Review: Internal Reward-Batch Prepare-for-Send Endpoint

## Goal

Add a non-production internal endpoint that prepares a `CREATED` reward batch
for the real send API by moving it to a safe past reference month.

The caller does not choose the month because it cannot know which batch
groupings already exist in the deployed environment. The service selects the
month and returns it to the caller.

This endpoint is test setup infrastructure. It must not implement or bypass
the invoice-replacement behavior under test. The functional test is expected
to remain red until the payment producer and the transactions consumer for the
invoice-replacement impact have been implemented.

The functional TDD test and deployment-layer controls are not part of the
`idpay-transactions` endpoint implementation task.

## Proposed API

```http
POST /idpay/internal/test-support/initiatives/{initiativeId}/reward-batches/{rewardBatchId}/prepare-for-send
```

The request has no body.

Successful response:

```http
200 OK
Content-Type: application/json

{
  "rewardBatchId": "5f52b84d-6f8c-4cf2-b070-44c7b23a78db",
  "previousMonth": "2026-08",
  "referenceMonth": "2026-07",
  "updateDate": "2026-08-28T11:45:00"
}
```

Keep the endpoint internal to `idpay-transactions`; do not expose it through
the public APIM specifications.

## Availability and security

- Register the controller only when `app.test-support.enabled=true`.
- Default the property to `false`.
- The deployment service owns environment enablement, ingress restrictions,
  and internal authentication.
- Do not treat omission from public APIM specifications as an authorization
  control.
- Log the batch ID, initiative ID, old month, and selected month without
  logging credentials or personal data.

## Month selection

The service selects a safe month; it must not require the functional test to
inspect existing reward batches.

Starting from the month before the current month in `Europe/Rome`, select the
nearest earlier month that:

- is not already used by another batch with the same initiative, merchant,
  and POS type; and
- has no earlier non-empty `CREATED` batch for the same initiative, merchant,
  and POS type that would cause the real send API to reject the request.

If the first candidate is occupied, continue with the preceding month. Do not
merge with, delete, overwrite, or otherwise modify the existing batch.

If the selected batch is already `CREATED` in a past month and the real send
preconditions are satisfied, return its current month without changing it.
This makes repeated calls idempotent.

## Validation and errors

The operation must fail when:

- the batch does not exist under the supplied initiative: `404 Not Found`;
- the batch status is not `CREATED`: `409 Conflict`; or
- no safe month can be selected within the configured test-support search
  horizon: `409 Conflict`.

Use stable application error codes and the existing error response shape.
Do not map unrelated data-integrity failures to a month-collision response.

## State change

Update only the selected batch:

- `month`;
- `startDate`: first day of the selected month at `00:00:00`;
- `endDate`: according to the existing reward-batch period convention;
- `name`: using the existing reward-batch naming helper; and
- `updateDate`: current timestamp.

Preserve its ID, status, merchant, initiative, POS type, associated
transactions, and all other lifecycle metadata. Batch amounts and counters are
derived from the currently associated transaction rows and therefore remain
unchanged.

Perform selection and update through a dedicated reactive SQL persistence
operation rather than a generic full-entity `save`.

## Suggested structure

- `TestRewardBatchController`
- `TestRewardBatchService`
- `PrepareRewardBatchForSendResponse`
- A dedicated persistence port and SQL adapter for the atomic operation

Reuse the existing batch date/name construction logic rather than duplicating
it. If that logic is private, extract a shared helper used by batch creation
and this test-support service.

## Tests

Add tests covering:

1. Successful preparation of a current-month `CREATED` batch.
2. Selection of the nearest safe past month.
3. Skipping a month whose grouping already exists.
4. Skipping months that would leave an earlier non-empty `CREATED` batch.
5. No merge, deletion, or modification of an existing target-month batch.
6. Idempotent repetition when the batch is already safely sendable.
7. Missing batch or initiative mismatch.
8. Batch not in `CREATED`.
9. Controller absent when test support is disabled.

## Functional-test flow enabled

1. Invoice a barcode transaction and read its original reward-batch ID.
2. Call this endpoint and retain the returned `referenceMonth`.
3. Call the real merchant endpoint:
   `POST /idpay/merchant/portal/initiatives/{initiativeId}/reward-batches/{batchId}/send`,
   including the required merchant context.
4. Update the invoice through the deployed `idpay-payment`
   initiative-scoped endpoint.
5. Wait for the asynchronous invoice-replacement impact with a bounded,
   diagnostic timeout.
6. Assert that the transaction:
   - no longer belongs to the source batch;
   - belongs to a different batch with the expected initiative, merchant, and
     POS type;
   - has in-batch status `SUSPENDED`; and
   - belongs to the month derived from the impact event `occurredAt` in
     `Europe/Rome`.

The test should initially fail at the missing invoice-replacement integration,
not during test setup. Its failure must distinguish an unobserved impact from
an impact that produced an incorrect batch transition.

## Acceptance criteria

- The endpoint has no request body and returns the month selected by the
  service.
- Existing target-month batches are never merged, deleted, or modified.
- Repeated preparation is idempotent.
- The endpoint is unreachable when test support is disabled.
- No public production API contract is changed.
- The real send validation and invoice-replacement paths remain untouched.
- The endpoint creates a deterministic sendable precondition without direct
  database access from functional tests.
