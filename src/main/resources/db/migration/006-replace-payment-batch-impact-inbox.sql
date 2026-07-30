ALTER TABLE reward_transactions
    ADD COLUMN latest_applied_payment_impact_revision BIGINT NOT NULL DEFAULT 0,
    ADD CONSTRAINT ck_reward_transactions_latest_impact_revision_non_negative
        CHECK (latest_applied_payment_impact_revision >= 0);

UPDATE reward_transactions transactions
SET latest_applied_payment_impact_revision = impacts.latest_applied_payment_impact_revision
FROM (
    SELECT transaction_id, MAX(transaction_revision) AS latest_applied_payment_impact_revision
    FROM reward_batch_impact_inbox
    GROUP BY transaction_id
) impacts
WHERE transactions.transaction_id = impacts.transaction_id;

DROP TABLE reward_batch_impact_inbox;
