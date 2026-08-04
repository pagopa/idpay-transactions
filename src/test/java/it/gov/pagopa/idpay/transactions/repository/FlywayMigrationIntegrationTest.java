package it.gov.pagopa.idpay.transactions.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class FlywayMigrationIntegrationTest {

    @Container
    private static final PostgreSQLContainer<?> postgresql = new PostgreSQLContainer<>("postgres:17-alpine")
            .withDatabaseName("idpay_transactions")
            .withUsername("idpay")
            .withPassword("idpay");

    @BeforeEach
    void resetSchema() {
        try (var connection = postgresql.createConnection("");
             var statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA public CASCADE");
            statement.execute("CREATE SCHEMA public");
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to reset the PostgreSQL test schema", exception);
        }
    }

    @Test
    void shouldRunMigrationsAtApplicationStartupWithTheApplicationDatabaseUser() {
        flywayApplication().run(context -> {
            Flyway flyway = context.getBean(Flyway.class);

            assertEquals(
                    "001,002,003,004,005,006,007",
                    java.util.Arrays.stream(flyway.info().applied())
                            .map(migration -> migration.getVersion().getVersion())
                            .collect(java.util.stream.Collectors.joining(","))
            );
            assertEquals(
                    "reports,reward_batches,reward_transactions",
                    query(flyway, """
                            SELECT table_name
                            FROM information_schema.tables
                            WHERE table_schema = 'public'
                              AND table_name IN ('reports', 'reward_batches', 'reward_transactions')
                            ORDER BY table_name
                            """)
            );
            assertEquals(
                    postgresql.getUsername(),
                    query(flyway, """
                            SELECT DISTINCT installed_by
                            FROM flyway_schema_history
                            WHERE success
                              AND version IS NOT NULL
                            """)
            );
        });
    }

    @Test
    void shouldNotReapplyMigrationsOnLaterApplicationStartups() {
        flywayApplication().run(context -> context.getBean(Flyway.class));
        flywayApplication().run(context -> {
            Flyway flyway = context.getBean(Flyway.class);

            assertEquals(
                    "7",
                    query(flyway, """
                            SELECT COUNT(*)
                            FROM flyway_schema_history
                            WHERE success
                              AND version IS NOT NULL
                            """)
            );
        });
    }

    private ApplicationContextRunner flywayApplication() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(FlywayAutoConfiguration.class))
                .withPropertyValues(
                        "spring.flyway.url=" + postgresql.getJdbcUrl(),
                        "spring.flyway.user=" + postgresql.getUsername(),
                        "spring.flyway.password=" + postgresql.getPassword()
                );
    }

    private String query(Flyway flyway, String sql) {
        try (var connection = flyway.getConfiguration().getDataSource().getConnection();
             var statement = connection.createStatement();
             var resultSet = statement.executeQuery(sql)) {
            StringBuilder values = new StringBuilder();
            while (resultSet.next()) {
                if (!values.isEmpty()) {
                    values.append(",");
                }
                values.append(resultSet.getString(1));
            }
            return values.toString();
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to query the Flyway test database", exception);
        }
    }
}
