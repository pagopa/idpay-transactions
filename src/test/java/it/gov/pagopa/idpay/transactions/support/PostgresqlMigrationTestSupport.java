package it.gov.pagopa.idpay.transactions.support;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.springframework.r2dbc.core.DatabaseClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

public abstract class PostgresqlMigrationTestSupport {

    @Container
    protected static final PostgreSQLContainer<?> postgresql = new PostgreSQLContainer<>("postgres:17-alpine")
            .withDatabaseName("idpay_transactions")
            .withUsername("idpay")
            .withPassword("idpay");

    private static DatabaseClient databaseClient;
    private static ConnectionFactory connectionFactory;

    protected static void applyRepositoryMigrations() {
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

        Flux.fromIterable(repositoryMigrationStatements())
                .concatMap(statement -> databaseClient.sql(statement).fetch().rowsUpdated())
                .then()
                .block();
    }

    protected static DatabaseClient databaseClient() {
        return databaseClient;
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

    private static List<String> repositoryMigrationStatements() {
        Path migrationsDirectory = Path.of("src", "main", "resources", "db", "migration");
        try (var migrations = Files.list(migrationsDirectory)) {
            return migrations
                    .filter(path -> path.getFileName().toString().endsWith(".sql"))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .flatMap(path -> sqlStatements(path).stream())
                    .toList();
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to read repository SQL migrations", exception);
        }
    }

    private static List<String> sqlStatements(Path migration) {
        try {
            return List.of(Files.readString(migration).split(";"))
                    .stream()
                    .map(String::trim)
                    .filter(statement -> !statement.isEmpty())
                    .toList();
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to read SQL migration " + migration, exception);
        }
    }
}
