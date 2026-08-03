CREATE TABLE reports (
    id TEXT PRIMARY KEY,
    initiative_id TEXT NOT NULL,
    report_status TEXT NOT NULL,
    start_period TIMESTAMP NOT NULL,
    end_period TIMESTAMP NOT NULL,
    merchant_id TEXT,
    business_name TEXT,
    request_date TIMESTAMP NOT NULL,
    elaboration_date TIMESTAMP,
    operator_level TEXT,
    file_name TEXT NOT NULL,
    report_type TEXT NOT NULL
);

CREATE INDEX idx_reports_initiative_type_request
    ON reports (initiative_id, report_type, request_date DESC);

CREATE INDEX idx_reports_merchant_initiative_type_request
    ON reports (merchant_id, initiative_id, report_type, request_date DESC)
    WHERE operator_level IS NULL;

CREATE INDEX idx_reports_operator_initiative_type_request
    ON reports (initiative_id, report_type, request_date DESC)
    WHERE operator_level IS NOT NULL;
