CREATE TABLE reward_batches (
    id VARCHAR(255) PRIMARY KEY,
    initiative_id VARCHAR(255) NOT NULL,
    merchant_id VARCHAR(255) NOT NULL,
    business_name VARCHAR(255),
    month CHAR(7) NOT NULL,
    pos_type VARCHAR(16) NOT NULL,
    status VARCHAR(32) NOT NULL,
    partial BOOLEAN NOT NULL DEFAULT FALSE,
    name VARCHAR(255) NOT NULL,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    approved_amount_cents BIGINT NOT NULL DEFAULT 0,
    suspended_amount_cents BIGINT NOT NULL DEFAULT 0,
    initial_amount_cents BIGINT NOT NULL DEFAULT 0,
    number_of_transactions BIGINT NOT NULL DEFAULT 0,
    number_of_transactions_elaborated BIGINT NOT NULL DEFAULT 0,
    report_path VARCHAR(1024),
    filename VARCHAR(1024),
    assignee_level VARCHAR(2) NOT NULL,
    number_of_transactions_suspended BIGINT NOT NULL DEFAULT 0,
    number_of_transactions_rejected BIGINT NOT NULL DEFAULT 0,
    refund_valuta_date DATE,
    refund_error_message TEXT,
    refund_outcome_timestamp TIMESTAMP,
    merchant_send_date TIMESTAMP,
    approval_date TIMESTAMP,
    creation_date TIMESTAMP NOT NULL,
    update_date TIMESTAMP NOT NULL,
    delivery_date_request TIMESTAMP,
    delivery_outcome JSONB,
    CONSTRAINT ck_reward_batches_month_format
        CHECK (month ~ '^[0-9]{4}-(0[1-9]|1[0-2])$'),
    CONSTRAINT ck_reward_batches_pos_type
        CHECK (pos_type IN ('PHYSICAL', 'ONLINE')),
    CONSTRAINT ck_reward_batches_status
        CHECK (status IN (
            'CREATED', 'SENT', 'EVALUATING', 'APPROVING', 'APPROVED',
            'PENDING_REFUND', 'NOT_REFUNDED', 'REFUNDED'
        )),
    CONSTRAINT ck_reward_batches_assignee_level
        CHECK (assignee_level IN ('L1', 'L2', 'L3')),
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

CREATE INDEX idx_reward_batches_merchant_initiative
    ON reward_batches (merchant_id, initiative_id);

CREATE INDEX idx_reward_batches_initiative_status
    ON reward_batches (initiative_id, status);

CREATE INDEX idx_reward_batches_initiative_assignee
    ON reward_batches (initiative_id, assignee_level);

CREATE INDEX idx_reward_batches_month
    ON reward_batches (month);

CREATE INDEX idx_reward_batches_delivery
    ON reward_batches (initiative_id, approved_amount_cents)
    WHERE status = 'APPROVED' AND approved_amount_cents > 0;

CREATE INDEX idx_reward_batches_pending_refund
    ON reward_batches (initiative_id)
    WHERE status = 'PENDING_REFUND';
