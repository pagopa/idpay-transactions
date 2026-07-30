ALTER TABLE reward_transactions
    ADD COLUMN transaction_revision BIGINT NOT NULL DEFAULT 0,
    ADD CONSTRAINT ck_reward_transactions_revision_non_negative
        CHECK (transaction_revision >= 0);

CREATE TABLE reward_batch_impact_inbox (
    event_id TEXT PRIMARY KEY,
    transaction_id TEXT NOT NULL,
    transaction_revision BIGINT NOT NULL,
    impact_type VARCHAR(64) NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uk_reward_batch_impact_inbox_transaction_revision
        UNIQUE (transaction_id, transaction_revision),
    CONSTRAINT ck_reward_batch_impact_inbox_revision_non_negative
        CHECK (transaction_revision >= 0)
);

CREATE INDEX idx_reward_batch_impact_inbox_transaction_revision
    ON reward_batch_impact_inbox (transaction_id, transaction_revision);
