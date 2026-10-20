ALTER TABLE reward_batches
    ADD COLUMN report_id TEXT;

CREATE INDEX idx_reward_batches_report_id
    ON reward_batches (report_id);

ALTER TABLE reward_batches
    ADD CONSTRAINT fk_reward_batches_report
        FOREIGN KEY (report_id)
            REFERENCES reports (id);
