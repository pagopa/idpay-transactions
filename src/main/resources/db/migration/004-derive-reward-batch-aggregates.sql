-- The reconciliation views are temporary cutover-audit artifacts. The external
-- audit described in docs/postgresql-schema-management.md must complete before
-- this migration removes the persisted legacy counters.
DROP VIEW reward_batch_counter_mismatches;
DROP VIEW reward_batch_counter_reconciliation;

-- Delivery selection uses the approved aggregate derived from assigned
-- transactions, so it cannot be represented by a reward_batches partial index.
DROP INDEX idx_reward_batches_delivery;

ALTER TABLE reward_batches
    DROP COLUMN approved_amount_cents,
    DROP COLUMN suspended_amount_cents,
    DROP COLUMN initial_amount_cents,
    DROP COLUMN number_of_transactions,
    DROP COLUMN number_of_transactions_elaborated,
    DROP COLUMN number_of_transactions_suspended,
    DROP COLUMN number_of_transactions_rejected;

ALTER TABLE reward_transactions
    ADD COLUMN accrued_reward_cents BIGINT NOT NULL,
    ADD CONSTRAINT ck_reward_transactions_accrued_reward_non_negative
        CHECK (accrued_reward_cents >= 0);

-- Covers grouped batch aggregates and state-specific aggregate projections
-- without extracting the reward amount from the JSONB payload.
DROP INDEX idx_reward_transactions_batch_status;
CREATE INDEX idx_reward_transactions_batch_status
    ON reward_transactions (reward_batch_id, reward_batch_trx_status)
    INCLUDE (accrued_reward_cents)
    WHERE reward_batch_id IS NOT NULL;
