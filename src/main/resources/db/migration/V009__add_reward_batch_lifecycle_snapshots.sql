ALTER TABLE reward_batches
    ADD COLUMN initial_amount_cents_at_send BIGINT NULL,
    ADD COLUMN suspended_amount_cents_at_approving BIGINT NULL;
