ALTER TABLE reward_batches
    ADD COLUMN last_outcome_check_date TIMESTAMP,
    ADD COLUMN last_invitalia_status TEXT,
    ADD COLUMN refund_amount_paid_cents BIGINT;

CREATE TABLE reward_batch_delivery_events (
                                              id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                              reward_batch_id TEXT NOT NULL,
                                              initiative_id TEXT NOT NULL,
                                              event_type TEXT NOT NULL CHECK (event_type IN ('DELIVERY', 'OUTCOME')),
                                              attempt_number INTEGER NOT NULL,
                                              outcome TEXT,
                                              invitalia_status TEXT,
                                              http_code INTEGER,
                                              amount_paid_cents BIGINT,
                                              request JSONB,
                                              response JSONB,
                                              created_at TIMESTAMP NOT NULL,
                                              CONSTRAINT fk_delivery_events_batch FOREIGN KEY (reward_batch_id, initiative_id)
                                                  REFERENCES reward_batches (id, initiative_id),
                                              CONSTRAINT uk_delivery_events_attempt UNIQUE (reward_batch_id, event_type, attempt_number)
);

CREATE TABLE reward_batch_delivery_transactions (
                                                    reward_batch_id TEXT NOT NULL,
                                                    transaction_id TEXT NOT NULL,
                                                    initiative_id TEXT NOT NULL,
                                                    accrued_reward_cents BIGINT NOT NULL,
                                                    reward_batch_trx_status TEXT NOT NULL,
                                                    created_at TIMESTAMP NOT NULL,
                                                    PRIMARY KEY (reward_batch_id, transaction_id),
                                                    CONSTRAINT fk_delivery_trx_batch FOREIGN KEY (reward_batch_id, initiative_id)
                                                        REFERENCES reward_batches (id, initiative_id)
);