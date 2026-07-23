package it.gov.pagopa.idpay.transactions.repository;

import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;

@Testcontainers(disabledWithoutDocker = true)
class RewardBatchDerivedAggregateMigrationTest extends PostgresqlMigrationTestSupport {

    @BeforeAll
    static void setUpDatabase() {
        applyRepositoryMigrations();
    }

    @AfterAll
    static void closeDatabase() {
        closeConnectionFactory();
    }

    @BeforeEach
    void clearDatabase() {
        databaseClient()
                .sql("DELETE FROM reward_transactions")
                .fetch()
                .rowsUpdated()
                .then(databaseClient()
                        .sql("DELETE FROM reward_batches")
                        .fetch()
                        .rowsUpdated())
                .block();
    }

    @Test
    void shouldRemoveLegacyCountersAndTemporaryReconciliationViews() {
        StepVerifier.create(databaseClient()
                        .sql("""
                                SELECT COUNT(*) AS count
                                FROM information_schema.columns
                                WHERE table_name = 'reward_batches'
                                  AND column_name IN (
                                      'approved_amount_cents',
                                      'suspended_amount_cents',
                                      'initial_amount_cents',
                                      'number_of_transactions',
                                      'number_of_transactions_elaborated',
                                      'number_of_transactions_suspended',
                                      'number_of_transactions_rejected'
                                  )
                                """)
                        .map((row, metadata) -> row.get("count", Long.class))
                        .one()
                        .zipWith(databaseClient()
                                .sql("""
                                        SELECT COUNT(*) AS count
                                        FROM information_schema.views
                                        WHERE table_name IN (
                                            'reward_batch_counter_reconciliation',
                                            'reward_batch_counter_mismatches'
                                        )
                                        """)
                                .map((row, metadata) -> row.get("count", Long.class))
                                .one())
                        .map(counts -> new Counts(counts.getT1(), counts.getT2())))
                .expectNext(new Counts(0L, 0L))
                .verifyComplete();
    }

    @Test
    void shouldDeriveBatchAmountsAndCountsFromTypedAccruedRewards() {
        insertBatchWithTransactions();

        StepVerifier.create(databaseClient()
                        .sql("""
                                SELECT
                                    COUNT(*) AS number_of_transactions,
                                    COALESCE(SUM(accrued_reward_cents), 0) AS initial_amount_cents,
                                    COUNT(*) FILTER (
                                        WHERE reward_batch_trx_status IN ('SUSPENDED', 'APPROVED', 'REJECTED')
                                    ) AS number_of_transactions_elaborated,
                                    COUNT(*) FILTER (
                                        WHERE reward_batch_trx_status = 'SUSPENDED'
                                    ) AS number_of_transactions_suspended,
                                    COUNT(*) FILTER (
                                        WHERE reward_batch_trx_status = 'REJECTED'
                                    ) AS number_of_transactions_rejected,
                                    COALESCE(SUM(accrued_reward_cents) FILTER (
                                        WHERE reward_batch_trx_status = 'SUSPENDED'
                                    ), 0) AS suspended_amount_cents,
                                    COALESCE(SUM(accrued_reward_cents) FILTER (
                                        WHERE reward_batch_trx_status IN ('TO_CHECK', 'CONSULTABLE', 'APPROVED')
                                    ), 0) AS approved_amount_cents
                                FROM reward_transactions
                                WHERE reward_batch_id = 'batch-1'
                                """)
                        .map((row, metadata) -> new BatchAggregate(
                                row.get("number_of_transactions", Long.class),
                                row.get("initial_amount_cents", Long.class),
                                row.get("number_of_transactions_elaborated", Long.class),
                                row.get("number_of_transactions_suspended", Long.class),
                                row.get("number_of_transactions_rejected", Long.class),
                                row.get("suspended_amount_cents", Long.class),
                                row.get("approved_amount_cents", Long.class)
                        ))
                        .one())
                .expectNext(new BatchAggregate(4L, 1000L, 3L, 1L, 1L, 200L, 400L))
                .verifyComplete();
    }

    @Test
    void shouldRequireNonNegativeTypedAccruedReward() {
        StepVerifier.create(databaseClient()
                        .sql("""
                                INSERT INTO reward_transactions (
                                    transaction_id, initiative_id, accrued_reward_cents
                                )
                                VALUES ('transaction-negative', 'initiative-1', -1)
                                """)
                        .fetch()
                        .rowsUpdated())
                .expectErrorMatches(throwable -> throwable.getMessage()
                        .contains("ck_reward_transactions_accrued_reward_non_negative"))
                .verify();
    }

    @Test
    void shouldCreateCoveringIndexForBatchAggregateQueries() {
        StepVerifier.create(databaseClient()
                        .sql("""
                                SELECT indexdef
                                FROM pg_indexes
                                WHERE schemaname = 'public'
                                  AND tablename = 'reward_transactions'
                                  AND indexname = 'idx_reward_transactions_batch_status'
                                """)
                        .map((row, metadata) -> row.get("indexdef", String.class))
                        .one())
                .expectNextMatches(indexDefinition ->
                        indexDefinition.contains("reward_batch_id, reward_batch_trx_status")
                                && indexDefinition.contains("INCLUDE (accrued_reward_cents)")
                                && indexDefinition.contains("WHERE (reward_batch_id IS NOT NULL)"))
                .verifyComplete();
    }

    private static void insertBatchWithTransactions() {
        StepVerifier.create(databaseClient()
                        .sql("""
                                INSERT INTO reward_batches (
                                    id, initiative_id, merchant_id, month, pos_type, status, name, assignee_level
                                )
                                VALUES (
                                    'batch-1', 'initiative-1', 'merchant-1', '2026-07', 'PHYSICAL',
                                    'EVALUATING', 'July batch', 'L1'
                                )
                                """)
                        .fetch()
                        .rowsUpdated()
                        .thenMany(databaseClient()
                                .sql("""
                                        INSERT INTO reward_transactions (
                                            transaction_id, initiative_id, reward_batch_id,
                                            reward_batch_trx_status, accrued_reward_cents
                                        )
                                        VALUES
                                            ('transaction-1', 'initiative-1', 'batch-1', 'TO_CHECK', 100),
                                            ('transaction-2', 'initiative-1', 'batch-1', 'APPROVED', 300),
                                            ('transaction-3', 'initiative-1', 'batch-1', 'REJECTED', 400),
                                            ('transaction-4', 'initiative-1', 'batch-1', 'SUSPENDED', 200)
                                        """)
                                .fetch()
                                .rowsUpdated()))
                .expectNext(1L, 4L)
                .verifyComplete();
    }

    private record Counts(Long legacyCounterColumns, Long reconciliationViews) {
    }

    private record BatchAggregate(
            Long numberOfTransactions,
            Long initialAmountCents,
            Long numberOfTransactionsElaborated,
            Long numberOfTransactionsSuspended,
            Long numberOfTransactionsRejected,
            Long suspendedAmountCents,
            Long approvedAmountCents
    ) {
    }
}
