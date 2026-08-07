ALTER TABLE reward_transactions
    ADD COLUMN transaction_revision BIGINT NOT NULL DEFAULT 0,
    ADD CONSTRAINT ck_reward_transactions_revision_non_negative
        CHECK (transaction_revision >= 0),
    ADD COLUMN latest_applied_payment_impact_revision BIGINT NOT NULL DEFAULT 0,
    ADD CONSTRAINT ck_reward_transactions_latest_impact_revision_non_negative
        CHECK (latest_applied_payment_impact_revision >= 0);
