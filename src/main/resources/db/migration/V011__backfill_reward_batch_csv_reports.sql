INSERT INTO reports (
    id,
    initiative_id,
    report_status,
    start_period,
    end_period,
    merchant_id,
    business_name,
    request_date,
    elaboration_date,
    operator_level,
    file_name,
    report_type
)
SELECT
    'reward-batch-csv-' || rb.id,
    rb.initiative_id,
    'GENERATED',
    rb.start_date,
    rb.end_date,
    rb.merchant_id,
    rb.business_name,
    rb.approval_date,
    rb.approval_date,
    'L3',
    rb.filename,
    'REWARD_BATCH_CSV'
FROM reward_batches rb
WHERE rb.report_id IS NULL
  AND rb.filename IS NOT NULL
  AND btrim(rb.filename) <> ''
  AND rb.start_date IS NOT NULL
  AND rb.end_date IS NOT NULL
  AND rb.approval_date IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM reports r
      WHERE r.id = 'reward-batch-csv-' || rb.id
  );

UPDATE reward_batches rb
SET report_id = 'reward-batch-csv-' || rb.id
WHERE rb.report_id IS NULL
  AND rb.filename IS NOT NULL
  AND btrim(rb.filename) <> ''
  AND rb.start_date IS NOT NULL
  AND rb.end_date IS NOT NULL
  AND rb.approval_date IS NOT NULL
  AND EXISTS (
      SELECT 1
      FROM reports r
      WHERE r.id = 'reward-batch-csv-' || rb.id
  );

