package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;

@Testcontainers(disabledWithoutDocker = true)
class RewardBatchesSchemaMigrationTest extends PostgresqlMigrationTestSupport {

    @BeforeAll
    static void setUpDatabase() {
        applyRepositoryMigrations();
    }

    @AfterAll
    static void closeDatabase() {
        closeConnectionFactory();
    }

    @Test
    void shouldCreateRewardBatchesTableWithGroupingConstraint() {
        StepVerifier.create(databaseClient()
                        .sql("""
                                SELECT constraint_name
                                FROM information_schema.table_constraints
                                WHERE table_name = 'reward_batches'
                                  AND constraint_name = 'uk_reward_batches_initiative_merchant_pos_month'
                                """)
                        .map((row, metadata) -> row.get("constraint_name", String.class))
                        .one())
                .expectNext("uk_reward_batches_initiative_merchant_pos_month")
                .verifyComplete();
    }
}
