-- Read-only migration-audit views. They do not alter batch or transaction data.
--
-- Expected clean result:
--   SELECT * FROM reward_batch_counter_mismatches;
-- returns no rows after an externally migrated dataset has consistent batch counters.
-- Counter deltas are persisted minus expected; a positive value means the batch
-- stores more than is derived from its assigned transactions.
--
-- approved_amount_cents is zero before evaluation. Once evaluation has started,
-- it is the accrued reward of TO_CHECK, CONSULTABLE, and APPROVED transactions.
-- Reassigned suspended transactions retain their suspension counters in CREATED
-- target batches, so the other status-based counters are checked in every state.
CREATE VIEW reward_batch_counter_reconciliation AS
WITH transaction_rewards AS (
    SELECT
        transaction_id,
        reward_batch_id,
        initiative_id,
        reward_batch_trx_status,
        CASE
            WHEN rewards -> initiative_id ->> 'accruedRewardCents' ~ '^-?[0-9]+$'
                THEN (rewards -> initiative_id ->> 'accruedRewardCents')::BIGINT
        END AS accrued_reward_cents
    FROM reward_transactions
    WHERE reward_batch_id IS NOT NULL
),
transaction_counters AS (
    SELECT
        reward_batch_id,
        initiative_id,
        COUNT(*) AS expected_number_of_transactions,
        COALESCE(SUM(accrued_reward_cents), 0)::BIGINT AS expected_initial_amount_cents,
        COUNT(*) FILTER (
            WHERE reward_batch_trx_status IN ('SUSPENDED', 'APPROVED', 'REJECTED')
        ) AS expected_number_of_transactions_elaborated,
        COUNT(*) FILTER (
            WHERE reward_batch_trx_status = 'SUSPENDED'
        ) AS expected_number_of_transactions_suspended,
        COUNT(*) FILTER (
            WHERE reward_batch_trx_status = 'REJECTED'
        ) AS expected_number_of_transactions_rejected,
        COALESCE(SUM(accrued_reward_cents) FILTER (
            WHERE reward_batch_trx_status = 'SUSPENDED'
        ), 0)::BIGINT AS expected_suspended_amount_cents,
        COALESCE(SUM(accrued_reward_cents) FILTER (
            WHERE reward_batch_trx_status IN ('TO_CHECK', 'CONSULTABLE', 'APPROVED')
        ), 0)::BIGINT AS evaluated_approved_amount_cents,
        COUNT(*) FILTER (
            WHERE accrued_reward_cents IS NULL
        ) AS transactions_without_accrued_reward,
        COUNT(*) FILTER (
            WHERE reward_batch_trx_status IS NULL
               OR reward_batch_trx_status NOT IN (
                   'TO_CHECK', 'CONSULTABLE', 'SUSPENDED', 'APPROVED', 'REJECTED'
               )
        ) AS transactions_with_unknown_batch_status
    FROM transaction_rewards
    GROUP BY reward_batch_id, initiative_id
)
SELECT
    batch.id AS reward_batch_id,
    batch.initiative_id,
    batch.status AS reward_batch_status,
    batch.number_of_transactions,
    COALESCE(counters.expected_number_of_transactions, 0) AS expected_number_of_transactions,
    batch.number_of_transactions - COALESCE(counters.expected_number_of_transactions, 0)
        AS number_of_transactions_delta,
    batch.initial_amount_cents,
    COALESCE(counters.expected_initial_amount_cents, 0) AS expected_initial_amount_cents,
    batch.initial_amount_cents - COALESCE(counters.expected_initial_amount_cents, 0)
        AS initial_amount_cents_delta,
    batch.number_of_transactions_elaborated,
    COALESCE(counters.expected_number_of_transactions_elaborated, 0)
        AS expected_number_of_transactions_elaborated,
    batch.number_of_transactions_elaborated
        - COALESCE(counters.expected_number_of_transactions_elaborated, 0)
        AS number_of_transactions_elaborated_delta,
    batch.number_of_transactions_suspended,
    COALESCE(counters.expected_number_of_transactions_suspended, 0)
        AS expected_number_of_transactions_suspended,
    batch.number_of_transactions_suspended
        - COALESCE(counters.expected_number_of_transactions_suspended, 0)
        AS number_of_transactions_suspended_delta,
    batch.number_of_transactions_rejected,
    COALESCE(counters.expected_number_of_transactions_rejected, 0)
        AS expected_number_of_transactions_rejected,
    batch.number_of_transactions_rejected
        - COALESCE(counters.expected_number_of_transactions_rejected, 0)
        AS number_of_transactions_rejected_delta,
    batch.suspended_amount_cents,
    COALESCE(counters.expected_suspended_amount_cents, 0) AS expected_suspended_amount_cents,
    batch.suspended_amount_cents - COALESCE(counters.expected_suspended_amount_cents, 0)
        AS suspended_amount_cents_delta,
    batch.approved_amount_cents,
    CASE
        WHEN batch.status IN ('EVALUATING', 'APPROVING', 'APPROVED', 'PENDING_REFUND', 'NOT_REFUNDED', 'REFUNDED')
            THEN COALESCE(counters.evaluated_approved_amount_cents, 0)
        ELSE 0
    END AS expected_approved_amount_cents,
    batch.approved_amount_cents - CASE
        WHEN batch.status IN ('EVALUATING', 'APPROVING', 'APPROVED', 'PENDING_REFUND', 'NOT_REFUNDED', 'REFUNDED')
            THEN COALESCE(counters.evaluated_approved_amount_cents, 0)
        ELSE 0
    END AS approved_amount_cents_delta,
    COALESCE(counters.transactions_without_accrued_reward, 0) AS transactions_without_accrued_reward,
    COALESCE(counters.transactions_with_unknown_batch_status, 0) AS transactions_with_unknown_batch_status
FROM reward_batches batch
LEFT JOIN transaction_counters counters
    ON counters.reward_batch_id = batch.id
   AND counters.initiative_id = batch.initiative_id;

CREATE VIEW reward_batch_counter_mismatches AS
SELECT *
FROM reward_batch_counter_reconciliation
WHERE number_of_transactions_delta <> 0
   OR initial_amount_cents_delta <> 0
   OR number_of_transactions_elaborated_delta <> 0
   OR number_of_transactions_suspended_delta <> 0
   OR number_of_transactions_rejected_delta <> 0
   OR suspended_amount_cents_delta <> 0
   OR approved_amount_cents_delta <> 0
   OR transactions_without_accrued_reward <> 0
   OR transactions_with_unknown_batch_status <> 0;
