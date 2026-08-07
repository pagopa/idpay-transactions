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

    @Test
    void shouldAllowNullAndPositiveDeliveryAmountButRejectNonPositiveValues() {
        StepVerifier.create(databaseClient()
                        .sql("""
                                INSERT INTO reward_batches (
                                    id, initiative_id, merchant_id, month, pos_type, status, name, assignee_level,
                                    delivery_amount_cents
                                ) VALUES
                                    ('delivery-null', 'initiative-schema', 'merchant', '2026-07', 'PHYSICAL', 'APPROVED', 'July', 'L3', NULL),
                                    ('delivery-positive', 'initiative-schema', 'merchant-2', '2026-07', 'PHYSICAL', 'APPROVED', 'July', 'L3', 1)
                                """)
                        .fetch()
                        .rowsUpdated()
                        .thenMany(databaseClient()
                                .sql("""
                                        SELECT delivery_amount_cents
                                        FROM reward_batches
                                        WHERE id = 'delivery-positive'
                                        """)
                                .map((row, metadata) -> row.get("delivery_amount_cents", Long.class))
                                .all()))
                .expectNext(1L)
                .verifyComplete();

        StepVerifier.create(databaseClient()
                        .sql("""
                                INSERT INTO reward_batches (
                                    id, initiative_id, merchant_id, month, pos_type, status, name, assignee_level,
                                    delivery_amount_cents
                                ) VALUES (
                                    'delivery-zero', 'initiative-schema', 'merchant-3', '2026-07',
                                    'PHYSICAL', 'APPROVED', 'July', 'L3', 0
                                )
                                """)
                        .fetch()
                        .rowsUpdated())
                .expectError()
                .verify();

        StepVerifier.create(databaseClient()
                        .sql("""
                                INSERT INTO reward_batches (
                                    id, initiative_id, merchant_id, month, pos_type, status, name, assignee_level,
                                    delivery_amount_cents
                                ) VALUES (
                                    'delivery-negative', 'initiative-schema', 'merchant-4', '2026-07',
                                    'PHYSICAL', 'APPROVED', 'July', 'L3', -1
                                )
                                """)
                        .fetch()
                        .rowsUpdated())
                .expectError()
                .verify();
    }
}
