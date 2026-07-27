package it.gov.pagopa.idpay.transactions.support;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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
    private static TransactionalOperator transactionalOperator;
    private static R2dbcEntityTemplate r2dbcEntityTemplate;

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
        r2dbcEntityTemplate = new R2dbcEntityTemplate(databaseClient, PostgresDialect.INSTANCE);
        transactionalOperator = TransactionalOperator.create(new R2dbcTransactionManager(connectionFactory));

        Flux.fromIterable(repositoryMigrationStatements())
                .concatMap(statement -> databaseClient.sql(statement).fetch().rowsUpdated())
                .then()
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

    static List<String> sqlStatements(Path migration) {
        try {
            String sql = Files.readString(migration);
            List<String> statements = new ArrayList<>();
            StringBuilder statement = new StringBuilder();
            boolean isLineComment = false;
            boolean isSingleQuoted = false;

            for (int index = 0; index < sql.length(); index++) {
                char current = sql.charAt(index);

                if (isLineComment) {
                    if (current == '\n') {
                        isLineComment = false;
                        statement.append(current);
                    }
                    continue;
                }

                if (isSingleQuoted) {
                    statement.append(current);
                    if (current == '\'') {
                        if (index + 1 < sql.length() && sql.charAt(index + 1) == '\'') {
                            statement.append(sql.charAt(++index));
                        } else {
                            isSingleQuoted = false;
                        }
                    }
                    continue;
                }

                if (current == '-' && index + 1 < sql.length() && sql.charAt(index + 1) == '-') {
                    isLineComment = true;
                    index++;
                } else if (current == '\'') {
                    isSingleQuoted = true;
                    statement.append(current);
                } else if (current == ';') {
                    addStatement(statements, statement);
                } else {
                    statement.append(current);
                }
            }

            addStatement(statements, statement);
            return statements;
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to read SQL migration " + migration, exception);
        }
    }

    private static void addStatement(List<String> statements, StringBuilder statement) {
        String value = statement.toString().trim();
        if (!value.isEmpty()) {
            statements.add(value);
        }
        statement.setLength(0);
    }
}
