CREATE TABLE reward_batches (
    id TEXT PRIMARY KEY,
    initiative_id TEXT NOT NULL,
    merchant_id TEXT NOT NULL,
    business_name TEXT,
    month CHAR(7) NOT NULL,
    pos_type TEXT NOT NULL,
    -- Kept as text: application validation permits additive statuses without enum rollout coupling.
    status VARCHAR(32) NOT NULL,
    partial BOOLEAN NOT NULL DEFAULT FALSE,
    name TEXT NOT NULL,

    start_date TIMESTAMP,
    end_date TIMESTAMP,
    creation_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    merchant_send_date TIMESTAMP,
    approval_date TIMESTAMP,
    delivery_date_request TIMESTAMP,
    refund_outcome_timestamp TIMESTAMP,
    approved_amount_cents BIGINT NOT NULL DEFAULT 0,
    suspended_amount_cents BIGINT NOT NULL DEFAULT 0,
    initial_amount_cents BIGINT NOT NULL DEFAULT 0,
    number_of_transactions BIGINT NOT NULL DEFAULT 0,
    number_of_transactions_elaborated BIGINT NOT NULL DEFAULT 0,
    number_of_transactions_suspended BIGINT NOT NULL DEFAULT 0,
    number_of_transactions_rejected BIGINT NOT NULL DEFAULT 0,

    report_path TEXT,
    filename TEXT,
    assignee_level TEXT NOT NULL,
    refund_valuta_date DATE,
    refund_error_message TEXT,
    delivery_outcome JSONB,

    CONSTRAINT ck_reward_batches_approved_amount_non_negative
        CHECK (approved_amount_cents >= 0),
    CONSTRAINT ck_reward_batches_suspended_amount_non_negative
        CHECK (suspended_amount_cents >= 0),
    CONSTRAINT ck_reward_batches_initial_amount_non_negative
        CHECK (initial_amount_cents >= 0),
    CONSTRAINT ck_reward_batches_transactions_non_negative
        CHECK (number_of_transactions >= 0),
    CONSTRAINT ck_reward_batches_elaborated_non_negative
        CHECK (number_of_transactions_elaborated >= 0),
    CONSTRAINT ck_reward_batches_suspended_transactions_non_negative
        CHECK (number_of_transactions_suspended >= 0),
    CONSTRAINT ck_reward_batches_rejected_transactions_non_negative
        CHECK (number_of_transactions_rejected >= 0),
    CONSTRAINT uk_reward_batches_initiative_merchant_pos_month
        UNIQUE (initiative_id, merchant_id, pos_type, month)
);

-- Merchant-facing batch lists and prior-month checks.
CREATE INDEX idx_reward_batches_merchant_initiative_month
    ON reward_batches (merchant_id, initiative_id, month DESC);

-- Initiative lifecycle queues, with database-side pagination by month.
CREATE INDEX idx_reward_batches_initiative_status_month
    ON reward_batches (initiative_id, status, month DESC);

CREATE INDEX idx_reward_batches_initiative_assignee_status_month
    ON reward_batches (initiative_id, assignee_level, status, month DESC);

-- Small partial indexes target background delivery and outcome polling.
CREATE INDEX idx_reward_batches_delivery
    ON reward_batches (initiative_id, approved_amount_cents)
    WHERE status = 'APPROVED' AND approved_amount_cents > 0;

CREATE INDEX idx_reward_batches_pending_refund
    ON reward_batches (initiative_id)
    WHERE status = 'PENDING_REFUND';