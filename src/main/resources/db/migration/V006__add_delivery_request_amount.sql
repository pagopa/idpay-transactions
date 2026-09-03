ALTER TABLE reward_batches
    ADD COLUMN delivery_amount_cents BIGINT,
    ADD CONSTRAINT ck_reward_batches_delivery_amount_non_negative
        CHECK (delivery_amount_cents IS NULL OR delivery_amount_cents > 0);
