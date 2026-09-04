-- Refund and cancellation events can carry a negative accrued reward.
ALTER TABLE reward_transactions
    DROP CONSTRAINT ck_reward_transactions_accrued_reward_non_negative;
