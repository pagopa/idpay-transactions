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
    void shouldCreateNullableBigintLifecycleSnapshotColumns() {
        StepVerifier.create(databaseClient()
                        .sql("""
                                SELECT column_name, data_type, udt_name, is_nullable
                                FROM information_schema.columns
                                WHERE table_schema = 'idpay-rimborsi'
                                  AND table_name = 'reward_batches'
                                  AND column_name IN (
                                      'initial_amount_cents_at_send',
                                      'suspended_amount_cents_at_approving'
                                  )
                                ORDER BY column_name
                                """)
                        .map((row, metadata) -> new SnapshotColumn(
                                row.get("column_name", String.class),
                                row.get("data_type", String.class),
                                row.get("udt_name", String.class),
                                row.get("is_nullable", String.class)
                        ))
                        .all())
                .expectNext(
                        new SnapshotColumn("initial_amount_cents_at_send", "bigint", "int8", "YES"),
                        new SnapshotColumn("suspended_amount_cents_at_approving", "bigint", "int8", "YES")
                )
                .verifyComplete();
    }

    @Test
    void shouldPersistNullAndZeroLifecycleSnapshotValuesAsDistinctStates() {
        StepVerifier.create(databaseClient()
                        .sql("""
                                INSERT INTO reward_batches (
                                    id, initiative_id, merchant_id, month, pos_type, status, name, assignee_level,
                                    initial_amount_cents_at_send, suspended_amount_cents_at_approving
                                ) VALUES
                                    ('snapshot-null', 'initiative-snapshot', 'merchant-null', '2026-07',
                                     'PHYSICAL', 'CREATED', 'July', 'L1', NULL, NULL),
                                    ('snapshot-zero', 'initiative-snapshot', 'merchant-zero', '2026-07',
                                     'PHYSICAL', 'CREATED', 'July', 'L1', 0, 0)
                                """)
                        .fetch()
                        .rowsUpdated()
                        .thenMany(databaseClient()
                                .sql("""
                                        SELECT id, initial_amount_cents_at_send, suspended_amount_cents_at_approving
                                        FROM reward_batches
                                        WHERE id IN ('snapshot-null', 'snapshot-zero')
                                        ORDER BY id
                                        """)
                                .map((row, metadata) -> new SnapshotValues(
                                        row.get("id", String.class),
                                        row.get("initial_amount_cents_at_send", Long.class),
                                        row.get("suspended_amount_cents_at_approving", Long.class)
                                ))
                                .all()))
                .expectNext(
                        new SnapshotValues("snapshot-null", null, null),
                        new SnapshotValues("snapshot-zero", 0L, 0L)
                )
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

    private record SnapshotColumn(String name, String dataType, String udtName, String nullable) {
    }

    private record SnapshotValues(
            String id,
            Long initialAmountCentsAtSend,
            Long suspendedAmountCentsAtApproving
    ) {
    }
}
