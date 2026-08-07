# PostgreSQL schema management

The application uses Flyway at startup to apply PostgreSQL schema migrations.
Its R2DBC connection remains the runtime data-access connection; Flyway uses a
JDBC connection to the same database with the same application credentials.

The versioned SQL files under `src/main/resources/db/migration/` are the
canonical schema artifacts. Flyway records applied versions in
`flyway_schema_history` and safely skips them on later application startups.

## Runtime configuration

| Variable | Default | Description |
| --- | --- | --- |
| `POSTGRES_JDBC_URL` | `jdbc:postgresql://localhost:5432/idpay-transactions` | PostgreSQL JDBC connection URL used by Flyway at startup. |
| `POSTGRES_R2DBC_URL` | `r2dbc:postgresql://localhost:5432/idpay-transactions` | PostgreSQL R2DBC connection URL. |
| `POSTGRES_R2DBC_SSL_MODE` | `PREFER` | R2DBC PostgreSQL SSL mode. Deployments should use `REQUIRE` or a certificate-verifying mode. |
| `POSTGRES_USERNAME` | `idpay` | PostgreSQL runtime user. |
| `POSTGRES_PASSWORD` | `idpay` | PostgreSQL runtime user password. |

The legacy `POSTGRES_URL_R2DBC` and `POSTGRES_URL_JDBC` names remain accepted
as fallbacks for existing deployments.

Credentials must be supplied through the deployment secret mechanism. The
same `POSTGRES_USERNAME` and `POSTGRES_PASSWORD` values are used by both
Flyway and R2DBC; a separate migration user is not required.

## External legacy-counter audit

The external migration process must provision an audit database through
migrations `V001` through `V003`, load the legacy batches and transactions, and
verify that:

```sql
SELECT *
FROM reward_batch_counter_mismatches;
```

returns no rows. The query compares the Mongo-derived batch counters with
aggregates calculated from assigned SQL transactions. A non-empty result,
missing typed reward value, or unknown in-batch status requires external
remediation; this service must not select a value or batch membership
arbitrarily.

After the audit is clean, start the application against an empty final target
database so Flyway applies migrations `V001` through `V007`. Then backfill
batches, transactions, and reports before cutover.

The transaction backfill must load `accrued_reward_cents` for every transaction
as a non-negative typed value. Migration `004` removes the temporary views and
mutable counter columns, so it must not be applied to the populated audit
database. The report backfill must preserve each report ID, initiative, scope
(`merchant_id` or `operator_level`), period, status, filename, and request and
elaboration dates so report list, download, retry, and force-generation APIs
remain available after cutover.

Thereafter, batch amounts and counts are read from SQL aggregates over assigned
transactions; the application neither runs this audit nor persists counters.
