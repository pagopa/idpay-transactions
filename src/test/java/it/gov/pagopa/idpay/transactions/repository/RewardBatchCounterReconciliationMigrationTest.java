package it.gov.pagopa.idpay.transactions.repository;

import io.r2dbc.postgresql.codec.Json;
import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;

@Testcontainers(disabledWithoutDocker = true)
class RewardBatchCounterReconciliationMigrationTest extends PostgresqlMigrationTestSupport {

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
    void shouldReportNoMismatchesWhenStoredCountersMatchAssignedTransactions() {
        insertReconciledBatch();

        StepVerifier.create(databaseClient()
                        .sql("SELECT reward_batch_id FROM reward_batch_counter_mismatches")
                        .map((row, metadata) -> row.get("reward_batch_id", String.class))
                        .all())
                .verifyComplete();
    }

    @Test
    void shouldExposeCounterDeltasWhenStoredCountersDoNotMatchAssignedTransactions() {
        insertReconciledBatch();

        StepVerifier.create(databaseClient()
                        .sql("""
                                UPDATE reward_batches
                                SET approved_amount_cents = approved_amount_cents + 1
                                WHERE id = 'batch-1'
                                """)
                        .fetch()
                        .rowsUpdated()
                        .then(databaseClient()
                                .sql("""
                                        SELECT reward_batch_id, approved_amount_cents_delta
                                        FROM reward_batch_counter_mismatches
                                        """)
                                .map((row, metadata) -> new CounterMismatch(
                                        row.get("reward_batch_id", String.class),
                                        row.get("approved_amount_cents_delta", Long.class)
                                ))
                                .one()))
                .expectNext(new CounterMismatch("batch-1", 1L))
                .verifyComplete();
    }

    private static void insertReconciledBatch() {
        StepVerifier.create(databaseClient()
                        .sql("""
                                INSERT INTO reward_batches (
                                    id, initiative_id, merchant_id, month, pos_type, status, name, assignee_level,
                                    initial_amount_cents, approved_amount_cents, suspended_amount_cents,
                                    number_of_transactions, number_of_transactions_elaborated,
                                    number_of_transactions_suspended, number_of_transactions_rejected
                                )
                                VALUES (
                                    'batch-1', 'initiative-1', 'merchant-1', '2026-07', 'PHYSICAL',
                                    'EVALUATING', 'July batch', 'L1', 1000, 400, 200, 4, 3, 1, 1
                                )
                                """)
                        .fetch()
                        .rowsUpdated()
                        .thenMany(databaseClient()
                                .sql("""
                                        INSERT INTO reward_transactions (
                                            transaction_id, initiative_id, reward_batch_id,
                                            reward_batch_trx_status, rewards
                                        )
                                        VALUES
                                            ('transaction-1', 'initiative-1', 'batch-1', 'TO_CHECK', :toCheckReward),
                                            ('transaction-2', 'initiative-1', 'batch-1', 'APPROVED', :approvedReward),
                                            ('transaction-3', 'initiative-1', 'batch-1', 'REJECTED', :rejectedReward),
                                            ('transaction-4', 'initiative-1', 'batch-1', 'SUSPENDED', :suspendedReward)
                                        """)
                                .bind("toCheckReward", Json.of("""
                                        {"initiative-1":{"accruedRewardCents":100}}
                                        """))
                                .bind("approvedReward", Json.of("""
                                        {"initiative-1":{"accruedRewardCents":300}}
                                        """))
                                .bind("rejectedReward", Json.of("""
                                        {"initiative-1":{"accruedRewardCents":400}}
                                        """))
                                .bind("suspendedReward", Json.of("""
                                        {"initiative-1":{"accruedRewardCents":200}}
                                        """))
                                .fetch()
                                .rowsUpdated()))
                .expectNext(1L, 4L)
                .verifyComplete();
    }

    private record CounterMismatch(String rewardBatchId, Long approvedAmountCentsDelta) {
    }
}
