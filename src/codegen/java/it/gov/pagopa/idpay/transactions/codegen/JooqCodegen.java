package it.gov.pagopa.idpay.transactions.codegen;

import org.jooq.codegen.GenerationTool;
import org.jooq.meta.jaxb.Configuration;
import org.jooq.meta.jaxb.Database;
import org.jooq.meta.jaxb.Generator;
import org.jooq.meta.jaxb.Jdbc;
import org.jooq.meta.jaxb.Target;
import org.testcontainers.containers.PostgreSQLContainer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;

public final class JooqCodegen {

    private JooqCodegen() {
    }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 1) {
            throw new IllegalArgumentException("Expected the generated-sources directory");
        }

        try (PostgreSQLContainer<?> postgresql = new PostgreSQLContainer<>("postgres:17-alpine")) {
            postgresql.start();
            applyRepositoryMigrations(postgresql);
            GenerationTool.generate(configuration(postgresql, arguments[0]));
        }
    }

    private static void applyRepositoryMigrations(PostgreSQLContainer<?> postgresql) throws Exception {
        try (Connection connection = openConnection(postgresql);
             var migrations = Files.list(Path.of("src", "main", "resources", "db", "migration"))) {
            for (Path migration : migrations
                    .filter(path -> path.getFileName().toString().endsWith(".sql"))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .toList()) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute(Files.readString(migration));
                }
            }
        }
    }

    private static Connection openConnection(PostgreSQLContainer<?> postgresql) throws InterruptedException, SQLException {
        SQLException failure = null;
        for (int attempt = 0; attempt < 20; attempt++) {
            try {
                return DriverManager.getConnection(
                        postgresql.getJdbcUrl(),
                        postgresql.getUsername(),
                        postgresql.getPassword()
                );
            } catch (SQLException exception) {
                failure = exception;
                Thread.sleep(250);
            }
        }
        throw failure;
    }

    private static Configuration configuration(PostgreSQLContainer<?> postgresql, String outputDirectory) {
        return new Configuration()
                .withJdbc(new Jdbc()
                        .withDriver("org.postgresql.Driver")
                        .withUrl(postgresql.getJdbcUrl())
                        .withUser(postgresql.getUsername())
                        .withPassword(postgresql.getPassword()))
                .withGenerator(new Generator()
                        .withDatabase(new Database()
                                .withName("org.jooq.meta.postgres.PostgresDatabase")
                                .withInputSchema("public")
                                .withIncludes("reward_batches|reward_transactions|reports"))
                        .withTarget(new Target()
                                .withPackageName("it.gov.pagopa.idpay.transactions.persistence.sql.generated")
                                .withDirectory(outputDirectory)));
    }
}
