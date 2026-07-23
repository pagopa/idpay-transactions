package it.gov.pagopa.idpay.transactions.support;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PostgresqlMigrationTestSupportTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void sqlStatements_ignoresSemicolonsInLineCommentsAndQuotedStrings() throws IOException {
        Path migration = temporaryDirectory.resolve("001-test.sql");
        Files.writeString(migration, """
                -- A comment with a semicolon; must not split the statement.
                CREATE VIEW first_view AS SELECT 'value; still quoted';
                -- Another comment; with a semicolon.
                CREATE VIEW second_view AS SELECT 2;
                """);

        assertEquals(
                List.of(
                        "CREATE VIEW first_view AS SELECT 'value; still quoted'",
                        "CREATE VIEW second_view AS SELECT 2"
                ),
                PostgresqlMigrationTestSupport.sqlStatements(migration)
        );
    }
}
