package it.gov.pagopa.idpay.transactions.persistence.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.gov.pagopa.idpay.transactions.support.PostgresqlMigrationTestSupport;
import java.time.Duration;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.connection.TransactionAwareConnectionFactoryProxy;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Testcontainers(disabledWithoutDocker = true)
class SqlRewardBatchRowLockTest extends PostgresqlMigrationTestSupport {

    private static final String INITIATIVE_ID = "initiative-1";
    private static final String MERCHANT_ID = "merchant-1";
    private static final String SOURCE_BATCH_ID = "source-batch";
    private static final String TARGET_BATCH_ID = "target-batch";
    private static final String TRANSACTION_ID = "transaction-1";

    private static DSLContext dslContext;

    @BeforeAll
    static void setUpDatabase() {
        applyRepositoryMigrations();
        dslContext = DSL.using(
                new TransactionAwareConnectionFactoryProxy(connectionFactory()),
                SQLDialect.POSTGRES
        );
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
    void shouldRejectAnEmptyBatchLockRequest() {
        StepVerifier.create(SqlRewardBatchRowLock.acquire(
                        dslContext,
                        INITIATIVE_ID,
                        java.util.List.of()
                ))
                .expectErrorMatches(error -> error instanceof IllegalArgumentException
                        && error.getMessage().contains("At least one reward batch ID"))
                .verify();
    }

    @Test
    void shouldRejectAMissingSingleBatch() {
        StepVerifier.create(SqlRewardBatchRowLock.acquireSingle(
                        dslContext,
                        INITIATIVE_ID,
                        SOURCE_BATCH_ID
                ))
                .expectError(SqlMembershipChangedException.class)
                .verify();
    }

    @Test
    void shouldRejectAMissingPairMember() {
        StepVerifier.create(insertBatch(SOURCE_BATCH_ID)
                        .then(SqlRewardBatchRowLock.acquirePair(
                                dslContext,
                                INITIATIVE_ID,
                                SOURCE_BATCH_ID,
                                TARGET_BATCH_ID
                        )))
                .expectError(SqlMembershipChangedException.class)
                .verify();
    }

    @Test
    void shouldRejectAMissingSourceWhenThePairTargetStillExists() {
        StepVerifier.create(insertBatch(TARGET_BATCH_ID)
                        .then(SqlRewardBatchRowLock.acquirePair(
                                dslContext,
                                INITIATIVE_ID,
                                SOURCE_BATCH_ID,
                                TARGET_BATCH_ID
                        )))
                .expectError(SqlMembershipChangedException.class)
                .verify();
    }

    @Test
    void shouldRejectABatchPairOutsideTheRequestedInitiative() {
        StepVerifier.create(insertBatch(SOURCE_BATCH_ID, "other-initiative")
                        .then(insertBatch(TARGET_BATCH_ID))
                        .then(SqlRewardBatchRowLock.acquirePair(
                                dslContext,
                                INITIATIVE_ID,
                                SOURCE_BATCH_ID,
                                TARGET_BATCH_ID
                        )))
                .expectError(SqlMembershipChangedException.class)
                .verify();
    }

    @Test
    void shouldAcquireBatchPairAndTransactionForTheExpectedMembership() {
        StepVerifier.create(insertBatch(SOURCE_BATCH_ID)
                        .then(insertBatch(TARGET_BATCH_ID))
                        .then(insertTransaction())
                        .then(SqlRewardBatchRowLock.acquirePair(
                                dslContext,
                                INITIATIVE_ID,
                                SOURCE_BATCH_ID,
                                TARGET_BATCH_ID
                        )))
                .assertNext(pair -> {
                    assertEquals(SOURCE_BATCH_ID, pair.source().getId());
                    assertEquals(TARGET_BATCH_ID, pair.target().getId());
                })
                .verifyComplete();

        StepVerifier.create(SqlRewardBatchRowLock.acquireTransaction(
                        dslContext,
                        TRANSACTION_ID,
                        INITIATIVE_ID,
                        SOURCE_BATCH_ID
                ))
                .assertNext(transaction -> {
                    assertEquals(TRANSACTION_ID, transaction.getTransactionId());
                    assertEquals(INITIATIVE_ID, transaction.getInitiativeId());
                    assertEquals(SOURCE_BATCH_ID, transaction.getRewardBatchId());
                })
                .verifyComplete();
    }

    @Test
    void shouldAcquireBatchRowsInAscendingIdOrderBeforeLockingHigherIds() {
        String lowerBatchId = "a-first-batch";
        String higherBatchId = "z-second-batch";

        StepVerifier.create(insertBatch(lowerBatchId)
                        .then(insertBatch(higherBatchId)))
                .verifyComplete();

        SqlBatchLockTestSupport.HeldRowLock lowerBatchLock = SqlBatchLockTestSupport
                .holdBatch(connectionFactory(), lowerBatchId, INITIATIVE_ID)
                .block();
        var pending = transactionalOperator()
                .transactional(SqlRewardBatchRowLock.acquire(
                        dslContext,
                        INITIATIVE_ID,
                        List.of(higherBatchId, lowerBatchId, higherBatchId)
                ))
                .toFuture();
        try {
            boolean blockedOnLowerBatch = SqlBatchLockTestSupport.blockedByWithin(
                    connectionFactory(),
                    lowerBatchLock.backendPid()
            ).block();

            StepVerifier.create(databaseClient()
                            .sql("""
                                    UPDATE reward_batches
                                    SET name = 'Higher batch updated while lower lock is held'
                                    WHERE id = :batchId
                                    """)
                            .bind("batchId", higherBatchId)
                            .fetch()
                            .rowsUpdated()
                            .timeout(Duration.ofSeconds(5)))
                    .expectNext(1L)
                    .verifyComplete();

            assertTrue(blockedOnLowerBatch,
                    "the acquisition must wait on the lower ID before attempting the higher ID");
        } finally {
            lowerBatchLock.release().block();
        }

        StepVerifier.create(Mono.fromFuture(pending).timeout(Duration.ofSeconds(5)))
                .assertNext(locked -> assertEquals(
                        List.of(lowerBatchId, higherBatchId),
                        locked.keySet().stream().sorted().toList()
                ))
                .verifyComplete();
    }

    @Test
    void shouldRejectTransactionWithUnexpectedMembership() {
        StepVerifier.create(insertBatch(SOURCE_BATCH_ID)
                        .then(insertTransaction())
                        .then(SqlRewardBatchRowLock.acquireTransaction(
                                dslContext,
                                TRANSACTION_ID,
                                INITIATIVE_ID,
                                TARGET_BATCH_ID
                        )))
                .expectError(SqlMembershipChangedException.class)
                .verify();
    }

    @Test
    void shouldRejectAMissingTransactionWhileAcquiringItsRowLock() {
        StepVerifier.create(SqlRewardBatchRowLock.acquireTransaction(
                        dslContext,
                        TRANSACTION_ID,
                        INITIATIVE_ID,
                        null
                ))
                .expectError(SqlMembershipChangedException.class)
                .verify();
    }

    private static Mono<Void> insertBatch(String batchId) {
        return insertBatch(batchId, INITIATIVE_ID);
    }

    private static Mono<Void> insertBatch(String batchId, String initiativeId) {
        return databaseClient()
                .sql("""
                        INSERT INTO reward_batches (
                            id, initiative_id, merchant_id, business_name, month, pos_type, status, name,
                            assignee_level
                        )
                        VALUES (
                            :id, :initiativeId, :merchantId, 'Business', '2026-04', 'PHYSICAL', 'CREATED',
                            'Batch', 'L1'
                        )
                        """)
                .bind("id", batchId)
                .bind("initiativeId", initiativeId)
                .bind("merchantId", MERCHANT_ID + "-" + batchId)
                .fetch()
                .rowsUpdated()
                .then();
    }

    private static Mono<Void> insertTransaction() {
        return databaseClient()
                .sql("""
                        INSERT INTO reward_transactions (
                            transaction_id, initiative_id, merchant_id, reward_batch_id, status,
                            reward_batch_trx_status, accrued_reward_cents
                        )
                        VALUES (
                            :transactionId, :initiativeId, :merchantId, :batchId, 'REWARDED',
                            'CONSULTABLE', 100
                        )
                        """)
                .bind("transactionId", TRANSACTION_ID)
                .bind("initiativeId", INITIATIVE_ID)
                .bind("merchantId", MERCHANT_ID)
                .bind("batchId", SOURCE_BATCH_ID)
                .fetch()
                .rowsUpdated()
                .then();
    }
}
