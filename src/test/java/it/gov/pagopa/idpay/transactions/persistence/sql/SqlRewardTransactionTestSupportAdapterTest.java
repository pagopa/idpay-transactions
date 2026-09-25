package it.gov.pagopa.idpay.transactions.persistence.sql;

import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
@Testcontainers(disabledWithoutDocker = true)
class SqlRewardTransactionTestSupportAdapterTest extends PostgresqlMigrationTestSupport {

    static SqlRewardTransactionTestSupportAdapter adapter;

    @BeforeAll
    static void setUpDatabase() {
        applyRepositoryMigrations();
        adapter = new SqlRewardTransactionTestSupportAdapter(
                transactionalOperator(),
                new R2dbcRepositoryFactory(r2dbcEntityTemplate())
                        .getRepository(RewardTransactionSqlRepository.class)
        );
    }

    @AfterAll
    static void closeDatabase() {
        closeConnectionFactory();
    }

    @BeforeEach
    void clearDatabase() {
        databaseClient().sql("DELETE FROM reward_transactions").fetch().rowsUpdated()
                .then(databaseClient().sql("DELETE FROM reward_batches").fetch().rowsUpdated())
                .block();
    }

    @Test
    void deleteByRewardBatchIdAndInitiativeIdReturningIds() {
        String rewardBatchId = "batch1";
        String initiativeId = "initiative1";

        StepVerifier.create(
                        databaseClient().sql("""
                                        INSERT INTO reward_batches (
                                            id, initiative_id, merchant_id, month, pos_type,
                                            status, name, assignee_level
                                        ) VALUES (
                                            :id, :initiativeId, 'merchant1', '2026-09', 'PHYSICAL',
                                            'CREATED', 'September 2026', 'L1'
                                        )
                                        """)
                                .bind("id", rewardBatchId)
                                .bind("initiativeId", initiativeId)
                                .fetch().rowsUpdated()
                                .then(databaseClient().sql("""
                                        INSERT INTO reward_transactions (
                                            transaction_id, reward_batch_id, initiative_id
                                        ) VALUES (:transactionId, :rewardBatchId, :initiativeId)
                                        """)
                                        .bind("transactionId", "trx1")
                                        .bind("rewardBatchId", rewardBatchId)
                                        .bind("initiativeId", initiativeId)
                                        .fetch().rowsUpdated())
                                .then(adapter.deleteByRewardBatchIdAndInitiativeIdReturningIds(
                                        rewardBatchId, initiativeId
                                ).collectList())
                )
                .assertNext(deletedIds -> {
                    assertEquals(List.of("trx1"), deletedIds);
                })
                .verifyComplete();

        StepVerifier.create(databaseClient().sql("""
                        SELECT COUNT(*) AS transaction_count
                        FROM reward_transactions
                        WHERE transaction_id = :transactionId
                        """)
                .bind("transactionId", "trx1")
                .map((row, metadata) -> row.get("transaction_count", Long.class))
                .one())
                .assertNext(count -> assertEquals(0L, count))
                .verifyComplete();
    }
}