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
