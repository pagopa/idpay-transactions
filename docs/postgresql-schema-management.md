# PostgreSQL schema management

The application connects to PostgreSQL only through R2DBC. It does not include
Flyway, a JDBC driver, schema initialization configuration, or a schema-history
table.

The ordered SQL files under `src/main/resources/db/migration/` are the
canonical schema artifacts. The deployment environment must apply those files
before an application version that depends on them starts. Testcontainers tests
may apply the files to their isolated database without recording migration
history.

## Runtime configuration

| Variable | Default | Description |
| --- | --- | --- |
| `POSTGRES_R2DBC_URL` | `r2dbc:postgresql://localhost:5432/idpay-transactions` | PostgreSQL R2DBC connection URL. |
| `POSTGRES_USERNAME` | `idpay` | PostgreSQL runtime user. |
| `POSTGRES_PASSWORD` | `idpay` | PostgreSQL runtime user password. |

Credentials must be supplied through the deployment secret mechanism. The
application does not manage the schema and must not receive a JDBC URL or
database migration credentials.

## External legacy-counter audit

The external migration process must provision an audit database through
migrations `001` through `003`, load the legacy batches and transactions, and
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

After the audit is clean, the external process provisions the final target
database by applying the ordered migrations `001` through `007` to an empty
database. It must then backfill batches, transactions, and reports before the
cutover application starts.

The transaction backfill must load `accrued_reward_cents` for every transaction
as a non-negative typed value. Migration `004` removes the temporary views and
mutable counter columns, so it must not be applied to the populated audit
database. The report backfill must preserve each report ID, initiative, scope
(`merchant_id` or `operator_level`), period, status, filename, and request and
elaboration dates so report list, download, retry, and force-generation APIs
remain available after cutover.

Thereafter, batch amounts and counts are read from SQL aggregates over assigned
transactions; the application neither runs this audit nor persists counters.
