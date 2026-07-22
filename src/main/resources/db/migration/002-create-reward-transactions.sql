ALTER TABLE reward_batches
    ADD CONSTRAINT uk_reward_batches_id_initiative UNIQUE (id, initiative_id);

CREATE TABLE reward_transactions (
    transaction_id TEXT PRIMARY KEY,
    initiative_id TEXT NOT NULL,
    reward_batch_id TEXT,

    id_trx_acquirer TEXT,
    acquirer_code TEXT,
    trx_date TIMESTAMP,
    hpan TEXT,
    operation_type TEXT,
    circuit_type TEXT,
    id_trx_issuer TEXT,
    correlation_id TEXT,
    amount_cents BIGINT,
    amount_currency TEXT,

    mcc TEXT,
    acquirer_id TEXT,
    merchant_id TEXT,
    point_of_sale_id TEXT,
    terminal_id TEXT,
    bin TEXT,
    sender_code TEXT,
    fiscal_code TEXT,
    vat TEXT,
    pos_type TEXT,
    par TEXT,
    status TEXT,
    rejection_reasons JSONB,
    initiative_rejection_reasons JSONB,
    rewards JSONB,

    user_id TEXT,
    masked_pan TEXT,
    brand_logo TEXT,
    operation_type_transcoded TEXT,
    effective_amount_cents BIGINT,
    trx_charge_date TIMESTAMP,
    refund_info JSONB,

    elaboration_date_time TIMESTAMP,
    channel TEXT,
    additional_properties JSONB,
    invoice_data JSONB,
    credit_note_data JSONB,
    trx_code TEXT,

    reward_batch_trx_status TEXT,
    reward_batch_rejection_reasons JSONB,
    reward_batch_inclusion_date TIMESTAMP,
    franchise_name TEXT,
    point_of_sale_type TEXT,
    business_name TEXT,
    invoice_upload_date TIMESTAMP,
    sampling_key INTEGER NOT NULL DEFAULT 0,
    update_date TIMESTAMP,
    extended_authorization BOOLEAN,
    voucher_amount_cents BIGINT,
    reward_batch_last_month_elaborated CHAR(7),
    checks_error JSONB,

    CONSTRAINT fk_reward_transactions_batch_initiative
        FOREIGN KEY (reward_batch_id, initiative_id)
        REFERENCES reward_batches (id, initiative_id)
);

-- Batch evaluation, decision, approval, CSV, and reassignment queries.
CREATE INDEX idx_reward_transactions_batch_status
    ON reward_transactions (reward_batch_id, reward_batch_trx_status);

CREATE INDEX idx_reward_transactions_batch_sampling
    ON reward_transactions (reward_batch_id, sampling_key);

-- Transaction and invoice lifecycle lookups.
CREATE INDEX idx_reward_transactions_issuer_user_date_amount
    ON reward_transactions (id_trx_issuer, user_id, trx_date, amount_cents);

CREATE INDEX idx_reward_transactions_user_date
    ON reward_transactions (user_id, trx_date);

CREATE INDEX idx_reward_transactions_initiative_merchant_status
    ON reward_transactions (initiative_id, merchant_id, status);

CREATE INDEX idx_reward_transactions_initiative_merchant_pos
    ON reward_transactions (initiative_id, merchant_id, point_of_sale_id);

CREATE INDEX idx_reward_transactions_initiative_merchant_fiscal_code
    ON reward_transactions (initiative_id, merchant_id, fiscal_code);

CREATE INDEX idx_reward_transactions_initiative_merchant_trx_code
    ON reward_transactions (initiative_id, merchant_id, trx_code);

CREATE INDEX idx_reward_transactions_invoiced_without_batch
    ON reward_transactions (initiative_id, merchant_id, transaction_id)
    WHERE status = 'INVOICED' AND reward_batch_id IS NULL;
