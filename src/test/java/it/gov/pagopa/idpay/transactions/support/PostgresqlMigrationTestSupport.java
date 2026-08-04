package it.gov.pagopa.idpay.transactions.support;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

public abstract class PostgresqlMigrationTestSupport {

    @Container
    protected static final PostgreSQLContainer<?> postgresql = new PostgreSQLContainer<>("postgres:17-alpine")
            .withDatabaseName("idpay_transactions")
            .withUsername("idpay")
            .withPassword("idpay");

    private static DatabaseClient databaseClient;
    private static ConnectionFactory connectionFactory;
    private static TransactionalOperator transactionalOperator;
    private static R2dbcEntityTemplate r2dbcEntityTemplate;

    protected static MigrateResult applyRepositoryMigrations() {
        initializeConnections();
        return repositoryFlyway().migrate();
    }

    protected static Flyway repositoryFlyway() {
        return Flyway.configure()
                .dataSource(postgresql.getJdbcUrl(), postgresql.getUsername(), postgresql.getPassword())
                .locations("classpath:db/migration")
                .load();
    }

    private static void initializeConnections() {
        ConnectionFactoryOptions options = ConnectionFactoryOptions.parse(
                "r2dbc:postgresql://%s:%d/%s".formatted(
                        postgresql.getHost(),
                        postgresql.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT),
                        postgresql.getDatabaseName()
                )
        );
        connectionFactory = ConnectionFactories.get(
                ConnectionFactoryOptions.builder()
                        .from(options)
                        .option(ConnectionFactoryOptions.USER, postgresql.getUsername())
                        .option(ConnectionFactoryOptions.PASSWORD, postgresql.getPassword())
                        .build()
        );
        databaseClient = DatabaseClient.create(connectionFactory);
        r2dbcEntityTemplate = new R2dbcEntityTemplate(databaseClient, PostgresDialect.INSTANCE);
        transactionalOperator = TransactionalOperator.create(new R2dbcTransactionManager(connectionFactory));

        Mono.from(connectionFactory.create())
                .flatMap(connection -> Mono.from(connection.close()))
                .retryWhen(Retry.backoff(20, Duration.ofMillis(250)))
                .block();
    }

    protected static DatabaseClient databaseClient() {
        return databaseClient;
    }

    protected static TransactionalOperator transactionalOperator() {
        return transactionalOperator;
    }

    protected static R2dbcEntityTemplate r2dbcEntityTemplate() {
        return r2dbcEntityTemplate;
    }

    protected static ConnectionFactory connectionFactory() {
        return connectionFactory;
    }

    protected static void closeConnectionFactory() {
        if (connectionFactory instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception exception) {
                throw new IllegalStateException("Unable to close the R2DBC connection factory", exception);
            }
        }
    }

}
